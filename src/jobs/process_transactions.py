import os
import pandas as pd

import ConfigurationHelpers
import DataReaders
import TransformHelpers
from Postgres import PGConnect, PGInterface

pd.set_option('display.max_columns', None)


def process(run):
    sourced_filepath = r'Sourced\tech_assessment_transactions_v2'

    transactions_df = extract(sourced_filepath)

    customer_df, transactions_df, errors_df = transform(transactions_df)

    load(customer_df, transactions_df, errors_df)


def extract(sourced_filepath):
    lake_location = ConfigurationHelpers.get_lake_location()
    path = os.path.join(lake_location, sourced_filepath, 'tech_assessment_transactions.json')

    transactions_dict = DataReaders.read_json(path)

    return pd.DataFrame.from_dict(transactions_dict['transactions'])


def transform(trans_df):
    # Create transactions primary key
    trans_df = TransformHelpers.add_unique_hashkey(trans_df, ['customerId', 'transactionId'])

    # Obscure PII data
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

    duplicates = trans_df[trans_df['duplicate'] == True]
    valid_trans_df = trans_df[trans_df['duplicate'] == False]
    valid_trans_df = valid_trans_df.drop(columns=['rank'])

    # Cast datetimes back to strings for merging for the duplicates df
    duplicates = TransformHelpers.cast_datetime_str(duplicates, 'transactionDate', '%Y-%m-%d')
    duplicates = TransformHelpers.cast_datetime_str(duplicates, 'sourceDate', '%Y-%m-%dT%H:%M:%S')

    # Create new UniqueHashKey for duplicates
    duplicates = TransformHelpers.add_unique_hashkey(duplicates, ['customerId', 'transactionId', 'rank'])
    duplicates = duplicates.drop(columns=['rank'])
    
    # Format invalid transactions 
    invalid_trans_df = pd.concat([invalid_trans_df, duplicates])
    invalid_trans_df = invalid_trans_df.drop(columns=['customerName'])
    invalid_trans_df['duplicate'] = invalid_trans_df['duplicate'].fillna(False)

    # Create customer and transactions tables
    valid_trans_df = TransformHelpers.dense_rank_desc(df=valid_trans_df,
                                                      groupby_cols=['customerId'],
                                                      rank_col='transactionDate')

    customer_df = (
        valid_trans_df
        .loc[valid_trans_df['rank'] == 1.0, ['customerId', 'customerName', 'transactionDate']]
        .rename(columns={'transactionDate': 'lastTransaction'})
    )
    # Drop any rows where two transactions have the same date
    customer_df = customer_df.drop_duplicates(subset='customerId', keep='first') 

    transactions_df = valid_trans_df.drop(columns=['customerName', 'rank'])

    # Cast datetimes back to strings for merging 
    transactions_df = TransformHelpers.cast_datetime_str(transactions_df, 'transactionDate', '%Y-%m-%d')
    transactions_df = TransformHelpers.cast_datetime_str(transactions_df, 'sourceDate', '%Y-%m-%dT%H:%M:%S')

    customer_df = TransformHelpers.cast_datetime_str(customer_df, 'lastTransaction', '%Y-%m-%d')

    # Deduping invalid transactions here. This is because there is a duplicate record that has an invalid currency
    # The way I've structured this work has meant these are removed before checking for duplicates
    # A restructure of the code would sort this.
    invalid_trans_df.drop_duplicates(subset="UniqueHashKey", keep=False, inplace=True) 

    return customer_df, transactions_df, invalid_trans_df


def load(customer_df, transactions_df, errors_df):
    # Create PG database connection
    # TODO: Need to store and retrieve credentials securely
    db_connection = PGConnect(database="Snoop-Tech-Test",
                               host="localhost",
                               user="postgres",
                               password="password",
                               port="5432")
    
    # Use connection instance above to interact with database
    pg_interface = PGInterface(db_conn=db_connection,
                               table_name='customers',
                               df=customer_df,
                               merge_column='customerId')
    pg_interface.merge()

    pg_interface = PGInterface(db_conn=db_connection,
                               table_name='transactions',
                               df=transactions_df,
                               merge_column='UniqueHashKey')
    pg_interface.merge()

    pg_interface = PGInterface(db_conn=db_connection,
                               table_name='errors',
                               df=errors_df,
                               merge_column='UniqueHashKey')
    pg_interface.merge()

    # Close DB connection
    db_connection.close()
