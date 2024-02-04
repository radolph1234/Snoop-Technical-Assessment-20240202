from datetime import datetime
import re
import psycopg2
import pandas as pd
from importlib import import_module
from pprint import pprint



class PGConnect:

    def __init__(self,
                 database: str,
                 host: str,
                 user: str,
                 password: str,
                 port: str):
        self.database = database
        self.host = host
        self.user = user
        self.password = password
        self.port = port

        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(database=self.database,
                                     host=self.host,
                                     user=self.user,
                                     password=self.password,
                                     port=self.port)

        self.cur = self.conn.cursor()

    def get_conn(self):
         return self.conn, self.cur
    
    def close(self):
        self.conn.close()


class PGInterface:
    def __init__(self,
                 db_conn: any,
                 table_name: str,
                 df: pd.DataFrame,
                 merge_column: str,
                 update_columns: list = None):
        self.db_conn = db_conn
        self.table_name = table_name
        self.df = df
        self.merge_column = merge_column
        self.update_columns = update_columns

        self.conn, self.cur = self.db_conn.get_conn()

    def add_updated_date(self):
        self.df['meta_updated'] = datetime.now()

    def create_table_if_not_exists(self):
        try:
            sql = re.sub('\s+', ' ', open(f'src\database\CT_{self.table_name}.sql', 'r').read())

            self.cur.execute(sql)
            self.conn.commit()
        except:
            raise Exception(f'No CT SQL query found in src\database for table: {self.table_name}')

    def import_schema(self):
        try:
            self.schema = getattr(import_module('schemas'), self.table_name)
            self.ordered_columns = list(self.schema.keys())
        except:
            raise Exception(f'No schema found in schemas.py for table: {self.table_name}')
        
    def enforce_schema(self):
        # reindex df columns 
        self.df = self.df.reindex(columns=self.ordered_columns)

    def format_merge_query(self):
        """
        Formating a query like:

        INSERT INTO inventory (id, name, price, quantity)
        VALUES (1, 'A', 16.99, 120)
        ON CONFLICT(id) 
        DO UPDATE SET
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity;
        """

        merge_query = f"""
        INSERT INTO {self.table_name} ({', '.join(i.lower() for i in self.ordered_columns)})
        VALUES {', '.join([str(i) for i in list(self.df.to_records(index=False))])}
        ON CONFLICT ({self.merge_column.lower()}) 
        DO UPDATE SET {", ".join([f'"{col.lower()}" = EXCLUDED.{col.lower()}' for col in self.update_columns])};
        """

        self.merge_query = re.sub('\s+', ' ', merge_query)

    def merge(self):
        # Add updated date to dataframe
        self.add_updated_date()

        # Enforce schema
        self.import_schema()
        self.enforce_schema()

        # Create table if not exists
        self.create_table_if_not_exists()

        self.update_columns = (
            list(filter(lambda c: c not in (self.merge_column), self.df.columns)) 
            if self.update_columns == None 
            else self.update_columns.append("meta_updated")
        )

        self.format_merge_query()

        self.cur.execute(self.merge_query)
        self.conn.commit()
