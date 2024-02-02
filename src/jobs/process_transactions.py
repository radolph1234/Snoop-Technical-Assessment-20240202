import os
import json

import pandas as pd

import ConfigurationHelpers


def process():
    sourced_filepath = r'Sourced\tech_assessment_transactions_v2'

    transactions_df = extract(sourced_filepath)

    transform(transactions_df)


def extract(sourced_filepath):
    lake_location = ConfigurationHelpers.get_lake_location()
    path = os.path.join(lake_location, sourced_filepath, 'tech_assessment_transactions.json')

    with open(path, 'r') as f:
        transactions_dict = json.load(f)

    return pd.DataFrame.from_dict(transactions_dict['transactions'])


def transform(transactions_df):
    pass


def load():
    pass
