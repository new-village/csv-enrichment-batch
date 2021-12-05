""" io.py
"""
from .azure_blob_storage import azure_blob_storage as abs
import pandas as pd
import logging
import sqlite3


logger = logging.getLogger()
# Connecting to a SQLite database
conn = sqlite3.connect(':memory:')


def import_data(_params):
    # Load Data by file name from Azure blob storage
    content = abs().load_csv(_params['from'])

    # Get blob data from Azure blob storage
    df = pd.read_csv(content)
    logger.info('{0} by UTF8 is successfully loaded'.format(_params['from']))

    if _params['select']:
        # Extract Select definitions
        keys = _params['select'].keys()
        names = [val['name'] for val in _params['select'].values()]
        types = [val['type'] for val in _params['select'].values()]
        # Select needed columns
        df = df[keys]
        # Change column names
        df.rename(columns=dict(zip(keys, names)), inplace=True)
        # Cast column type
        df.astype(dict(zip(names, types)))

    # Load dataframe to sqlite
    df.to_sql(_params['create'], conn, index=False, if_exists='replace')
    return


def export_data(_params):
    df = pd.read_sql_query('SELECT * FROM {0}'.format(_params['from']), conn)
    print(df)
    return
