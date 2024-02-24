import yaml

import pandas as pd
from google.cloud import bigquery


MERGE_PURCH_DAYS_TH = 3
CHAIN_CONCAT_DAYS_TH = 10


def read_yaml(path: str) -> dict:
    """
    Read YAML data from a file.

    Parameters:
        - path (str): The path to the YAML file.

    Returns:
        - dict: A dictionary containing the parsed YAML data.
    """
    with open(path) as f:
        return yaml.safe_load(f)


def read_bigquery(client: bigquery.Client, query: str) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query using the provided client and return the results as a Pandas DataFrame.
    Column names in the resulting DataFrame are standardized to lowercase with spaces replaced by underscores.

    Parameters:
        - client (bigquery.Client): BigQuery client instance.
        - query (str): SQL query to be executed on BigQuery.

    Returns:
        - pd.DataFrame: DataFrame containing the results of the BigQuery query.
    """

    query_job = client.query(query)
    rows = query_job.result()
    df = pd.DataFrame().from_records([dict(row) for row in rows])
    df.columns = ['_'.join(c.lower().split()) for c in df.columns]
    return df


def chain_events_concatenation(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenate touchpoints into conversion chains based on customer sessions.
    Note: this function iterates through the events DataFrame, grouping touchpoints into conversion chains
        based on customer sessions. The resulting DataFrame includes information about each conversion,
        such as customer ID, conversion ID, conversion timestamp, concatenated touchpoint chain,
        first event timestamp, purchase date, and purchase value.

    Parameters:
        - events_df (pd.DataFrame): DataFrame containing event information.
            It should have columns like 'customer_id', 'session_id', 'channel_group', 'first_event_timestamp', etc.

    Returns:
        - pd.DataFrame: DataFrame with concatenated touchpoints organized into conversion chains.
            The resulting DataFrame includes columns for 'customer_id', 'conversion_id', 'conversion_timestamp',
            'chain', 'first_event', 'purchase_date', and 'purchase_value' representing the conversion chains.
    """

    i = 0
    conv_chains = {}

    for _, customer_df in events_df.groupby('customer_id'):
        j = 0
        conv_chain = []
        conv_chain_detail = {}
        nr_rows = len(customer_df)
        
        # reset index in order for index to range from 0 to nr_rows-1
        customer_df = customer_df.reset_index(drop=True)
            
        for k in range(nr_rows):
            row = customer_df.loc[k]
            current_session_id = customer_df.loc[k, 'session_id']
            # keep track of the session id of the next event in time order
            next_session_id = customer_df.loc[k + 1, 'session_id'] if k < nr_rows - 1 else 'FINISH'
            
            # add touchpoint to chain
            conv_chain.append(row['channel_group'])
            
            # if this is the first event of the chain then set conversion chain first event as the event's first event timestamp
            if j == 0:
                conv_chain_detail['first_event'] = row['first_event_timestamp'].date()
                j += 1

            # if the event corresponds to the last event of a converted session then add the chain to the customer's list of conversion chains 
            if (row['f_purchased'] == 1 and current_session_id != next_session_id):
                p_chain = conv_chain.copy()  # NOTA: se non viene copiata la chain allora modificandola negli step successivi viene modificata inplace anche nell'output
                conv_chain_detail['customer_id'] = row['customer_id']
                conv_chain_detail['conversion_id'] = row['session_id'] + '.' + row['last_event_date'].strftime('%Y%m%d')
                conv_chain_detail['conversion_timestamp'] = row['last_event_timestamp']
                conv_chain_detail['chain'] = p_chain
                conv_chain_detail['purchase_date'] = row['last_event_timestamp'].date()
                conv_chain_detail['purchase_value'] = row['session_revenue']

                conv_chains[i] = conv_chain_detail
                            
                # reset chain
                conv_chain = []  # comment if you want to keep tp corresponding to previous purchases, uncomment if not
                conv_chain_detail = {}
                
                # update indexes
                i += 1
                j = 0
                
    chains_df = pd.DataFrame(conv_chains.values())
    chains_df = chains_df[['customer_id', 'conversion_id', 'conversion_timestamp', 'chain', 'first_event', 'purchase_date', 'purchase_value']]
    assert events_df[events_df.f_purchased == 1].session_id.nunique() == len(chains_df) == chains_df.conversion_id.nunique(), 'ERROR! Found conversions with multiple assigned chains'
    assert events_df[events_df.f_purchased == 1].session_id.nunique() == chains_df.conversion_id.nunique(), f'ERROR! Mismatching number of conversions, found {chains_df[chains_df.f_purchased == 1].session_id.nunique()} in input data and {chains_df.conversion_id.nunique()} in chains data'
    return chains_df


def chain_merge(chains_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge consecutive chains based on a specified time threshold for each customer.
    Note: this function calculates ranks for events within each customer's chain based on the time difference.
        It then merges consecutive chains if the time difference exceeds the threshold specified by `MERGE_PURCH_DAYS_TH`.
        The resulting DataFrame ensures that each conversion has a unique assigned chain after merging.

    Parameters:
        - chains_df (pd.DataFrame): DataFrame containing chain information.
            It should have columns like 'conversion_id', 'customer_id', 'purchase_date', 'chain', etc.

    Returns:
        - pd.DataFrame: DataFrame with merged chains based on consecutive events within a time threshold.
            The resulting DataFrame includes columns for 'conversion_id', 'conversion_timestamp', 'first_event',
            'purchase_date', 'purchase_value', and 'chain' representing the merged chains.
    """
    def calculate_rank(group):
        rank = 1
        current_date = group['purchase_date'].iloc[0]

        for _, row in group.iterrows():
            if (row['purchase_date'] - current_date).days > MERGE_PURCH_DAYS_TH:
                rank += 1
            group.at[_, 'rank'] = rank
            current_date = row['purchase_date']
        return group

    merged_chains_df = chains_df.copy()
    merged_chains_df['rank'] = None
    merged_chains_df = merged_chains_df.sort_values(['customer_id', 'conversion_timestamp'], ascending=(True, True))
    merged_chains_df = merged_chains_df.groupby('customer_id').apply(calculate_rank).reset_index(drop=True)
    
    # aggregate chains
    merged_chains_df = merged_chains_df.groupby(['customer_id','rank']).agg({'conversion_id':'last', 'conversion_timestamp':'last', 'first_event':'min', 'purchase_date':'max', 'purchase_value':'sum', 'chain':'sum'}).reset_index()
    assert len(merged_chains_df) == merged_chains_df.conversion_id.nunique(), 'ERROR! Found conversions with multiple assigned chains'
    return merged_chains_df


def chain_concatenation(chains_df: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenate chains within a specified time range for each customer.
    Note: this function considers events within a time range of `CHAIN_CONCAT_DAYS_TH` days.
        The input DataFrame is expected to be sorted in descending order by 'purchase_date'.
        It ensures that each conversion has a unique assigned chain after concatenation.

    Parameters:
        - chains_df (pd.DataFrame): DataFrame containing chain information.
            It should have columns like 'conversion_id', 'customer_id', 'purchase_date', 'chain', etc.

    Returns:
        - pd.DataFrame: DataFrame with concatenated chains based on events within a specified time range.
            The resulting DataFrame includes a new column 'concat_chain' containing the concatenated chains.
    """
    def concatenate_events_within_range(group):
        for i, row in group.iterrows():
            current_date = row['purchase_date']
            current_chain = row['chain']

            for j, inner_row in group.iterrows():
                if (i != j) & (current_date > inner_row['purchase_date']) & (
                        (current_date - inner_row['purchase_date']).days <= CHAIN_CONCAT_DAYS_TH):
                    current_chain = inner_row['chain'] + current_chain

            group.at[i, 'concat_chain'] = current_chain

        return group
    
    concat_chains_df = chains_df.copy()
    concat_chains_df['concat_chain'] = None
    concat_chains_df = concat_chains_df.sort_values(['customer_id','purchase_date'], ascending=(True, False))  # sort descending by purchase date
    concat_chains_df = concat_chains_df.groupby('customer_id').apply(concatenate_events_within_range).reset_index(drop=True)
    assert len(concat_chains_df) == concat_chains_df.conversion_id.nunique(), 'ERROR! Found conversions with multiple assigned chains'
    return concat_chains_df


def compute_channel_stats(chains_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute channel statistics based on multi-touch attribution information.

    Parameters:
        - chains_df (pd.DataFrame): DataFrame containing multi-touch attribution information.
            It should have columns like 'conversion_id', 'chain', 'purchase_value', 'chain_len', etc.

    Returns:
        - pd.DataFrame: DataFrame with computed channel statistics ndexed by 'channel_name' including
            the number of appearances, number of chains, etc.
    """

    channels_df = chains_df.copy()
    channels_df = channels_df.explode('chain').reset_index(drop=True).reset_index()
    channels_df['order'] = channels_df.groupby('conversion_id')['index'].rank('first')
    # according to GA4 in case of a mono-touchpoint chain then the channel is considered as last interaction but not as first interaction
    # channels_df['first_channel'] = channels_df['order'].apply(lambda x: 1 if x == 1 else 0)
    channels_df['first_channel'] = channels_df.apply(lambda x: 1 if x['order'] == 1 and x['order'] < x['chain_len'] else 0, axis=1)
    channels_df['last_channel'] = channels_df.apply(lambda x: 1 if x['order'] == x['chain_len'] else 0, axis=1)
    # according to GA4 an assist is any interaction that is on the conversion path but is not the last interaction
    # channels_df['assist_channel'] = channels_df.apply(lambda x: 1 if 1 < x['order'] < x['chain_len'] else 0, axis=1)
    channels_df['assist_channel'] = channels_df.apply(lambda x: 1 if x['order'] < x['chain_len'] else 0, axis=1)
    channels_df['f_mono_touch'] = channels_df['chain_len'].apply(lambda x: 1 if x == 1 else 0)
    channels_df['first_touch_value'] = channels_df.apply(lambda x: x['purchase_value'] if x['first_channel'] == 1 else 0, axis=1)
    channels_df['last_touch_value'] = channels_df.apply(lambda x: x['purchase_value'] if x['last_channel'] == 1 else 0, axis=1)
    
    channel_stats = channels_df.groupby('chain').agg(
        nr_appearances=('conversion_id', 'count'),
        nr_chains=('conversion_id', 'nunique'),
        nr_mono_touch_chains=('f_mono_touch', 'sum'),
        nr_first_touch_chains=('first_channel', 'sum'),
        first_touch_chains_value=('first_touch_value', 'sum'),
        nr_last_touch_chains=('last_channel', 'sum'),
        last_touch_chains_value=('last_touch_value', 'sum')
    ).reset_index()
    assist_stats = channels_df[channels_df.assist_channel == 1][['chain', 'conversion_id', 'purchase_value']].drop_duplicates().groupby('chain').agg(
        nr_assisted_chains=('conversion_id', 'nunique'),
        assisted_chains_value=('purchase_value', 'sum')
    ).reset_index()
    mean_chains_len = channels_df[['conversion_id', 'chain', 'chain_len']].drop_duplicates().groupby('chain').agg(mean_chain_len=('chain_len', 'mean')).reset_index()
    # TODO: lunghezza mediana

    channel_stats = pd.merge(channel_stats, assist_stats, on='chain', how='outer')
    channel_stats = pd.merge(channel_stats, mean_chains_len, on='chain', how='outer').rename({'chain': 'channel_name'}, axis=1)

    # Assisted/Last Click or Direct Conversions and First/Last Click or Direct Conversions:
    channel_stats['assisted/last_touch'] = channel_stats['nr_assisted_chains'] / channel_stats['nr_last_touch_chains']
    channel_stats['first/last_touch'] = channel_stats['nr_first_touch_chains'] / channel_stats['nr_last_touch_chains']

    return channel_stats