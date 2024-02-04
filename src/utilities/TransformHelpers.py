from datetime import datetime
import hashlib


def sha2_hash(df, column):
    df[column] = df[column].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

    return df


def add_unique_hashkey(df, columns):
    df['UniqueHashKey'] = df[columns].astype(str).apply(''.join, axis=1)
    df = sha2_hash(df, 'UniqueHashKey')

    return df


def DQ_date(value):

    format = '%Y-%m-%d' if len(value) == 10 else '%Y-%m-%dT%H:%M:%S'

    try:
        dt = datetime.strptime(value, format)
        
        if dt <= datetime.now():
            return True
        
        return False
    
    except:
        return False


def DQ_currency(value):
    if value in ['EUR', 'GBP', 'USD']:
        return True
    else:
        return False
    

def cast_datetime(df, column, format):
    df[column] = df[column].apply(lambda x: datetime.strptime(x, format))

    return df

def cast_datetime_str(df, column, format):
    df[column] = df[column].dt.strftime(format)

    return df


def dense_rank_desc(df, groupby_cols, rank_col):
    df['rank'] = (
        df
        .groupby(groupby_cols)[rank_col]
        .rank(method='dense', ascending=False)
    )
    return df
