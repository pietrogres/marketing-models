import os
import time
import logging
import datetime as dt

import pandas as pd
from dateutil.relativedelta import relativedelta
from ChannelAttribution import markov_model, heuristic_models
from google.cloud import bigquery

from functions import (
    attribution_parser,
    read_yaml,
    read_locally,
    chain_events_concatenation,
    chain_merge,
    chain_concatenation,
    compute_perimeter_recap,
    compute_channel_stats
)


# parse arguments
ARGS = attribution_parser()
CONCAT_CHAINS = ARGS.concat_chains
FORCE_RECOMPUTE = ARGS.force_recompute

# i/o paths
ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC_PATH = os.path.join(ROOT_PATH, 'src')
DATA_PATH = os.path.join(SRC_PATH, 'data')
LOGS_PATH = os.path.join(SRC_PATH, 'logs')

# BigQuery key to read data
KEY_PATH = os.path.join(ROOT_PATH, 'sa-ml-attribution.json')


def main():

    # configure logging
    logging_filename = f'{dt.datetime.today().strftime("%Y%m%d%H%M")}_ga4_attribution_model.log'
    logging.basicConfig(
        filename=os.path.join(LOGS_PATH, logging_filename),
        level=logging.DEBUG,
        format='%(levelname)s %(asctime)s %(filename)s: %(funcName)s() %(lineno)d: \t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'
    )
    # to print logging also at console level
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

    logging.info('Starting Attribution model')
    logging.info(f'{"C" if CONCAT_CHAINS else "NOT c"}oncatenating conversion chains if purchases are near to each other\n')

    # configure BigQuery client
    client = bigquery.Client.from_service_account_json(KEY_PATH)

    try:
        start_time = time.time()

        logging.info('Execution step: data import')
        config = read_yaml(os.path.join(SRC_PATH, 'config', 'config.yaml'))
        query = f"""SELECT * FROM `{config['project']}.{config['dataset']}.{config['attribution_input_table']}`"""

        events = read_locally(client, query, os.path.join(DATA_PATH, 'attribution_events.parquet'), force_read=FORCE_RECOMPUTE)
        events['first_event_timestamp'] = pd.to_datetime(events['first_event_timestamp'])
        events['last_event_date'] = pd.to_datetime(events['last_event_timestamp']).apply(lambda x: x.date())
        assert events[events.channel_group.isna()].session_id.nunique() == 0, 'ERROR! Found sessions with missing channel group'
        assert events.groupby('session_id').agg(last_event_date=('last_event_date', 'nunique')).reset_index().last_event_date.max() == 1, 'ERROR! Found sessions with multiple last event dates'
        assert events.groupby('session_id').agg(f_converted=('f_converted', 'nunique')).reset_index().f_converted.max() == 1, 'ERROR! Found sessions with multiple conversion flags'
        logging.info(f'Input data consists of {len(events)} rows, {events.session_id.nunique()} unique sessions, {events[events.f_converted == 1].session_id.nunique()} unique conversions, {events.cookie_id.nunique()} unique customers')
        logging.info(f'Input data events range from {events.first_event_timestamp.min().date().strftime("%d/%m/%Y")} to {events.last_event_timestamp.max().date().strftime("%d/%m/%Y")}')
        logging.info(f'Conversions events range from {events[events.f_converted == 1].event_timestamp.min().date().strftime("%d/%m/%Y")} to {events[events.f_converted == 1].event_timestamp.max().date().strftime("%d/%m/%Y")}')

        logging.info('Execution step: chains creation')

        # sort sessions by first event timestamp
        events = events.sort_values(by=['cookie_id', 'first_event_timestamp', 'event_timestamp'], ascending=[True, True, True])

        # create conversion chains - step1
        # for every customer and every purchase create a list of touchpoint related to that purchase
        logging.info('Chain creation - step 1 - chain creation')
        chains_df = chain_events_concatenation(events)
        logging.info(f'Considering {len(chains_df)} unique chains with {round(chains_df.chain.map(len).mean(), 1)} avg length and {chains_df.cookie_id.nunique()} unique customers')

        # create conversion chains - step2
        # combine purchases and chains if purchase dates differ by a maximum of 3 days
        logging.info('Chain creation - step 2 - chain merge')
        merged_chains_df = chain_merge(chains_df)
        logging.info(f'{len(chains_df) - len(merged_chains_df)} chains have been merged ({(len(chains_df) - len(merged_chains_df)) * 100 / len(chains_df):.1f}%)')
        logging.info(f'Resulting {len(merged_chains_df)} unique chains with {round(merged_chains_df.chain.map(len).mean(), 1)} avg length and {merged_chains_df.cookie_id.nunique()} unique customers')

        # create conversion chains - step3
        # concatenate chains if purchase dates occurr within 10 days
        # note, purchases are kept separate here, only chains are concatenated
        if CONCAT_CHAINS:
            logging.info('Chain creation - step 3 - chain concatenation')
            concat_chains_df = chain_concatenation(chains_df)
            logging.info(f'{concat_chains_df.conversion_id.nunique()} resulting chains with {round(concat_chains_df.chain.map(len).mean(), 1)} raw avg length and {round(concat_chains_df.concat_chain.map(len).mean(), 1)} concat avg length, {concat_chains_df.cookie_id.nunique()} unique customers')

            preproc_chains_df = concat_chains_df.copy()
            preproc_chains_df = preproc_chains_df.drop(['chain'], axis=1).rename({'concat_chain': 'chain'}, axis=1)

        else:
            preproc_chains_df = merged_chains_df.copy()

        preproc_chains_df['chain_len'] = preproc_chains_df['chain'].map(len)
        preproc_chains_df['chain_duration'] = preproc_chains_df.apply(lambda x: relativedelta(x.purchase_date, x.first_event).days, axis=1)
        logging.info(f'Considering {preproc_chains_df.conversion_id.nunique()} chains with {round(preproc_chains_df.chain.map(len).mean(), 1)} avg length, {preproc_chains_df.cookie_id.nunique()} unique customers')
        logging.info(f'  {preproc_chains_df[preproc_chains_df.chain_len == 1].conversion_id.nunique()} mono-touchpoint chains ({round(preproc_chains_df[preproc_chains_df.chain_len == 1].conversion_id.nunique() * 100 / preproc_chains_df.conversion_id.nunique(), 2)}%)')

        logging.info(f'Writing preprocessed chains to {os.path.join(DATA_PATH, "preproc_chains.parquet")}...')
        preproc_chains_df.to_parquet(os.path.join(DATA_PATH, 'attribution_chains.parquet'))

        logging.info('Execution step: chains analysis')
        perimeter_recap = compute_perimeter_recap(preproc_chains_df)
        channel_stats = compute_channel_stats(preproc_chains_df)

        logging.info('Execution step: model')
        model_chains_df = preproc_chains_df.copy()
        model_chains_df['chain'] = model_chains_df['chain'].apply(lambda x: ' > '.join(x))
        model_chains_df = model_chains_df.groupby('chain').agg(nr_chains=('cookie_id', 'count'), total_revenue=('purchase_value', 'sum')).reset_index()
        model_chains_df['total_revenue'] = model_chains_df['total_revenue'].astype(float)

        # compute models
        markov_attr = markov_model(Data=model_chains_df, var_path='chain', var_conv='nr_chains', var_value='total_revenue', sep='>').rename({'total_conversions': 'markov_volume', 'total_conversion_value': 'markov_value'}, axis=1)
        heuristic_attr = heuristic_models(Data=model_chains_df, var_path='chain', var_conv='nr_chains', var_value='total_revenue')

        # combine results
        attribution_results = pd.merge(channel_stats, markov_attr, on='channel_name', how='outer')
        attribution_results = pd.merge(attribution_results, heuristic_attr, on='channel_name', how='outer')

        logging.info('Execution step: output')
        today = dt.date.today().strftime('%Y%m%d')
        logging.info(f'Writing attribution results to {os.path.join(DATA_PATH, f"{today}_attribution_results.xlsx")}...')
        writer = pd.ExcelWriter(os.path.join(DATA_PATH, f'{today}_attribution_results.xlsx'))
        perimeter_recap.to_excel(writer, sheet_name='Perimeter', index=False)
        attribution_results.to_excel(writer, sheet_name='Attribution', index=False)
        writer.close()

        logging.info(f'Execution time: {dt.timedelta(seconds=time.time() - start_time)}')

    except Exception as e:
        logging.error(e, exc_info=True)
        exit(1)


if __name__ == '__main__':
    main()
