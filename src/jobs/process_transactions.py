import os
import json
from datetime import datetime
import pandas as pd

import ConfigurationHelpers
import DataReaders
import TransformHelpers

pd.set_option('display.max_columns', None)


def process():
    sourced_filepath = r'Sourced\tech_assessment_transactions_v2'

    transactions_df = extract(sourced_filepath)

    customer_df, transactions_df, invalid_trans_df = transform(transactions_df)

    load(customer_df, transactions_df, invalid_trans_df)


def extract(sourced_filepath):
    lake_location = ConfigurationHelpers.get_lake_location()
    path = os.path.join(lake_location, sourced_filepath, 'tech_assessment_transactions.json')

    transactions_dict = DataReaders.read_json(path)

    return pd.DataFrame.from_dict(transactions_dict['transactions'])


def transform(trans_df):
    # Create transactions primary key
    trans_df = TransformHelpers.add_unique_hashkey(trans_df, ['customerId', 'transactionId'])

    # obscure PII data
    trans_df = TransformHelpers.sha2_hash(trans_df, 'customerName')

    # Perform DQ Checks
    trans_df['validTransactionDate'] = trans_df['transactionDate'].apply(TransformHelpers.DQ_date)
    trans_df['validSourceDate'] = trans_df['sourceDate'].apply(TransformHelpers.DQ_date)
    trans_df['validCurrency'] = trans_df['currency'].apply(TransformHelpers.DQ_currency)

    # Filter for valid rows
    invalid_trans_df = trans_df[(trans_df['validTransactionDate'] == False) |
                                (trans_df['validSourceDate'] == False) |
                                (trans_df['validCurrency'] == False)]
    
    trans_df = trans_df[(trans_df['validTransactionDate'] == True) &
                        (trans_df['validSourceDate'] == True) &
                        (trans_df['validCurrency'] == True)]

    # Cast datetime columns
    trans_df = TransformHelpers.cast_datetime(trans_df, 'transactionDate', '%Y-%m-%d')
    trans_df = TransformHelpers.cast_datetime(trans_df, 'sourceDate', '%Y-%m-%dT%H:%M:%S')
    
    # Check for duplicate transactions records
    trans_df = TransformHelpers.dense_rank_desc(df=trans_df,
                                                groupby_cols=['UniqueHashKey'],
                                                rank_col='sourceDate')

    trans_df['duplicate'] = trans_df['rank'].apply(lambda x: True if x > 1.0 else False)
    trans_df = trans_df.drop(columns=['rank'])

    valid_trans_df = trans_df[trans_df['duplicate'] == False]
    duplicates = trans_df[trans_df['duplicate'] == True]
    
    invalid_trans_df = pd.concat([invalid_trans_df, duplicates])
    invalid_trans_df = invalid_trans_df.drop(columns=['customerName'])

    # Create customer and transactions tables
    valid_trans_df = TransformHelpers.dense_rank_desc(df=trans_df,
                                                      groupby_cols=['customerId'],
                                                      rank_col='transactionDate')

    customer_df = (
        valid_trans_df
        .loc[valid_trans_df['rank'] == 1.0, ['customerId', 'customerName', 'transactionDate']]
        .rename(columns={'transactionDate': 'lastTransaction'})
    )

    transactions_df = valid_trans_df.drop(columns=['customerName', 'rank'])

    return customer_df, transactions_df, invalid_trans_df


def load(customer_df, transactions_df, invalid_trans_df):
    pass
