""" io.py
"""
from .abs import abs
#import dataset


def input(_p):
    # connecting to a SQLite database
    # db = dataset.connect('sqlite:///csv-enrichment-batch.db')
    # table = db[_p['create']]

    # Load Data
    data = abs().load_csv(_p['from'])
    print(data)
    return "input"


def output(data, params):
    print(params['file'])
    print(data)
    return "output"
