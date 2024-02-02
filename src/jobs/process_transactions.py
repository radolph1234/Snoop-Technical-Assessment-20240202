import os
import json

import pandas as pd

import ConfigurationHelpers
import DataReaders


def process():
    sourced_filepath = r'Sourced\tech_assessment_transactions_v2'

    transactions_df = extract(sourced_filepath)

    transform(transactions_df)


def extract(sourced_filepath):
    lake_location = ConfigurationHelpers.get_lake_location()
    path = os.path.join(lake_location, sourced_filepath, 'tech_assessment_transactions.json')

    transactions_dict = DataReaders.read_json(path)

    return pd.DataFrame.from_dict(transactions_dict['transactions'])


def transform(transactions_df):
    # Validate currency
    # Check for inValid dates
    # Check for duplicate transactions records
    transactions_df['rank'] = (
        transactions_df
        .groupby(['customerId', 'transactionId'])['sourceDate']
        .rank(ascending=True)
    )
    pass


def load():
    pass
