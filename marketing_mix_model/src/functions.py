import re
import json
import logging
import argparse
import datetime as dt

import yaml
import xlsxwriter
import pandas as pd


def mmm_parser() -> argparse.Namespace:
    """
    Parse command-line arguments for the Attribution model for Google Analytics 4 digital conversion.

    Returns:
        - argparse.Namespace: An object containing parsed command-line arguments.
    """
    choices = ['IT']
    parser = argparse.ArgumentParser(description='Evaluation of MMM model results')
    parser.add_argument('-c', '--country', help='MMM country in short format', required=True, choices=choices)
    return parser.parse_args()


def configure_log(logfile: str, logfmt: str = '%(asctime)s %(levelname)s %(filename)s %(lineno)d: \t%(message)s'):
    """
    Configure the logging settings for both file and console handlers.

    Parameters:
        - logfile (str): Path to the log file.
        - logfmt (str, optional): Format string for log messages.
            Defaults to '%(asctime)s %(levelname)s %(filename)s %(lineno)d: \t%(message)s'.
    """
    logging.basicConfig(
        filename=logfile,
        level=logging.DEBUG,
        format=logfmt,
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'
    )

    # create a StreamHandler for logging at console level
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(logfmt, datefmt='%Y-%m-%d %H:%M:%S')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


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


def read_json(path: str) -> dict:
    """
    Read JSON data from a file.

    Parameters:
        - path (str): The path to the JSON file.

    Returns:
        - dict: A dictionary containing the parsed JSON data.
    """

    with open(path, 'r') as f:
        return json.load(f)['InputCollect']


def map_year_rolling(d: dt.date, years_map: dict) -> str:
    """
    Map a given date to a specific year based on a rolling mapping defined by start and end dates.

    Args:
        - d (datetime.date): The date to be mapped.
        - years_map (dict): A dictionary defining the mapping of years with corresponding start and end dates.

    Returns:
        - str: The mapped year or 'oos' (out of scope) if the date doesn't fall within any specified range.
    """

    for year in years_map.keys():
        if years_map[year]['start_date'] <= d <= years_map[year]['end_date']:
            return year
    else:
        return 'oos'


def prepare_time_features(channels_df: pd.DataFrame, perimeter: tuple, years_map: dict) -> pd.DataFrame:
    """
    Prepare time-related features for the given DataFrame.

    This function filters the DataFrame based on the provided time perimeter,
    calculates additional time-related features such as 'year_rolling' and 'year_nr',
    and returns the modified DataFrame.

    Args:
        - channels_df (pd.DataFrame): The DataFrame containing the channels data.
        - perimeter (tuple): A tuple representing the time period boundary as (start_date, end_date).
        - years_map (dict): A dictionary mapping years to their start and end dates.

    Returns:
        - pd.DataFrame: The DataFrame with time-related features added and filtered based on the given perimeter.
    """

    df = channels_df.copy()
    df = df[df.week_end_date.between(*perimeter)]
    df['year_rolling'] = df['week_end_date'].apply(lambda x: map_year_rolling(x, years_map))
    df['year_nr'] = df['year_rolling'].apply(lambda x: int(re.findall(r'\d+', x)[0]))
    return df


def compute_mmm_input_channel_recap(channels_df: pd.DataFrame, ch_config: dict):
    """
    Compute a recap of input channels for Marketing Mix Modeling (MMM) analysis.

    This function takes a DataFrame containing channel data and a configuration
    dictionary specifying the prefixes, subchannels, and splits, and computes
    a recap of input channels suitable for MMM analysis.

    Notes:
        Columns containing pressure and investment data in the DataFrame must follow
        the naming convention <prefix>_<subchannel>_<split> where split is one of [grp, lea, imp, cli app, num, inv].

    Args:
        - channels_df (pd.DataFrame): The DataFrame containing the channel data.
        - ch_config (dict): A dictionary specifying the prefixes, subchannels, and splits.
            Example: {'prefix': 'channel', 'subchannels': ['sub1', 'sub2'], 'splits': ['grp', 'lea', 'imp']}

    Returns:
        - pd.DataFrame: A DataFrame containing the recap of input channels for MMM analysis.
    """

    splits_map = {'grp': 'grps', 'lea': 'leads', 'imp': 'impressions', 'cli': 'clicks', 'app': 'appointments', 'num': 'number', 'inv': 'cost'}
    prefix = ch_config['prefix']
    subchannels_list = ch_config['subchannels']
    splits_list = ch_config['splits']

    df_list = []

    for subch in subchannels_list:
        subch_cols = [f'{prefix}_{subch}_{split}' for split in splits_list]
        subch_cols_map = {col: splits_map[re.findall(r'.*\_(grp|lea|imp|cli|app|num|inv)', col)[0]] for col in subch_cols}

        subch_recap = channels_df[channels_df.year_rolling != 'oos'][['week_end_date', 'year_rolling'] + subch_cols].rename(subch_cols_map, axis=1)
        subch_recap['split'] = ' '.join(subch.upper().split('_'))
        df_list.append(subch_recap)

    ch_recap = pd.concat(df_list)

    splist_cols = [col for col in ch_recap.columns if col in splits_map.values()]
    ch_recap = ch_recap.pivot_table(index=['year_rolling', 'split'], values=splist_cols, aggfunc='sum').reset_index().sort_values(['year_rolling', 'split'])

    subch_cols = []
    cost_cols = []
    cost_yy_cols = []

    # create year number column
    ch_recap['year_nr'] = ch_recap['year_rolling'].apply(lambda x: int(re.findall(r'\d+', x)[0]))

    for col in splist_cols:

        # compute cost per pressure columns
        if col != 'cost':
            # overall
            cost_col = f'cost_per_{re.sub(r"s$", "", col)}'
            ch_recap[cost_col] = ch_recap['cost'] / ch_recap[col]
            cost_cols += [cost_col]

            # current year vs previous year
            ch_recap_prev_year = ch_recap.copy()
            ch_recap_prev_year['year_nr'] = ch_recap_prev_year['year_nr'] + 1
            ch_recap = pd.merge(ch_recap, ch_recap_prev_year[['year_nr', 'split', cost_col]].rename({cost_col: f'{cost_col}_y/y'}, axis=1), on=['year_nr', 'split'], how='left')
            ch_recap[f'{cost_col}_y/y'] = ch_recap[cost_col] / ch_recap[f'{cost_col}_y/y'] - 1
            cost_yy_cols += [f'{cost_col}_y/y']

        # compute pressure % columns
        ch_recap = pd.merge(ch_recap, ch_recap[ch_recap.split.str.contains('TOT ')][['year_rolling', col]].rename({col: f'{col}_perc'}, axis=1), on='year_rolling', how='left')
        ch_recap[f'{col}_perc'] = ch_recap[col] / ch_recap[f'{col}_perc']
        subch_cols += [col, f'{col}_perc']

    return ch_recap[['year_rolling', 'split'] + subch_cols + cost_cols + cost_yy_cols]


def write_format_excel(path, out_df_list, format_columns=False):
    def format_column(col):
        numeric_format = workbook.add_format({'num_format': '#,##0.0'})
        percentage_format = workbook.add_format({'num_format': '0.0%'})
        if col in df.select_dtypes('float'):
            return percentage_format if re.search(r'_perc_?|y/y$|\%$', col) else numeric_format
        else:
            return None

    def adapt_column_width(col):
        return max(
            df[col].astype(str).map(len).max(),  # len of largest item
            len(str(col))                        # len of column name/header
        ) + 1                                    # adding a little extra space

    def conditional_format_column_with_color_scale(df, column_name):
        column = df[column_name]
        min_value = min(column)
        max_value = max(column)
        pivot_value = (max_value + min_value) / 2

        last_column = len(df.index) + 1
        column_index = df.columns.get_loc(column_name)
        excel_column = xlsxwriter.utility.xl_col_to_name(column_index)
        column_to_format = f'{excel_column}2:{excel_column}{last_column}'

        color_format = {
            'type': '3_color_scale',
            'min_type': 'num',
            'min_value': min_value,
            'mid_type': 'num',
            'mid_value': pivot_value,
            'max_type': 'num',
            'max_value': max_value
        }

        sheet.conditional_format(column_to_format, color_format)

    writer = pd.ExcelWriter(path)
    workbook = writer.book

    for sheet_name, df in out_df_list.items():

        # write sheets
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        # format numeric and percentage cols
        sheet = writer.sheets[sheet_name]

        for col in df.columns:
            col_idx = df.columns.get_loc(col)
            col_letter = xlsxwriter.utility.xl_col_to_name(col_idx)

            # color column if it represent a percentage
            if format_columns and re.findall(r'\bperc\b|y/y$|\%$', col) != []:
                conditional_format_column_with_color_scale(df, col)

            sheet.set_column(f'{col_letter}:{col_letter}', width=adapt_column_width(col), cell_format=format_column(col))

    writer.close()
