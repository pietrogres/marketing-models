import os
import re
import logging
import datetime as dt

import pandas as pd

import src.functions as f


# parse arguments
ARGS = f.mmm_parser()
COUNTRY = ARGS.country

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_PATH = os.path.join(ROOT_PATH, 'src')
CONF_PATH = os.path.join(SRC_PATH, 'config')
DATA_PATH = os.path.join(SRC_PATH, 'data')
LOGS_PATH = os.path.join(SRC_PATH, 'logs')


def main() -> None:

    # configure logging
    f.configure_log(os.path.join(LOGS_PATH, f'attribution_log_{dt.datetime.now().strftime("%Y%m%d%H%M")}.log'))

    logging.info('Starting Marketing Mix Model (MMM) for country {COUNTRY}')

    # import county configuration details
    country_config = f.read_yaml(os.path.join(CONF_PATH, f'config_{COUNTRY.lower()}.yaml'))
    mmm_perimeter = country_config['mmm_perimeter']['start_date'], country_config['mmm_perimeter']['end_date']
    years_map = country_config['years_map']
    logging.info(f'MMM perimeter ranging from {mmm_perimeter[0].date()} to {mmm_perimeter[1].date()}\n')

    try:

        logging.info(f'Execution step: Data import')
        preproc_data = pd.read_csv(os.path.join(DATA_PATH, f'MMM_{COUNTRY}_input_data.csv'), sep=',')
        preproc_data['week_end_date'] = pd.to_datetime(preproc_data['week_end_date'], format='%d/%m/%Y')
        preproc_data = f.prepare_time_features(preproc_data, mmm_perimeter, years_map)
        preproc_data = preproc_data[preproc_data.year_rolling != 'oos']

        ch_recap_map = {}
        logging.info(f'Execution step: Input channel recap')
        for ch, ch_config in country_config['date_preprocessing_report'].items():
            logging.info(f'\tComputing {ch} channel recap')
            ch_recap_map[ch] = f.compute_mmm_input_channel_recap(preproc_data, ch_config)

        out_date = dt.date.today().strftime('%Y%m%d')
        logging.info(f'Execution step: Writing output to {DATA_PATH}')
        f.write_format_excel(os.path.join(DATA_PATH, f'{out_date}_MMM_{COUNTRY}_input_channels_recap.xlsx'), ch_recap_map)
                
    except Exception as e:
        logging.error(e, exc_info=True)
        exit(1)


if __name__ == '__main__':
    main()
