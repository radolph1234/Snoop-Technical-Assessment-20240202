from datetime import datetime
import re
import psycopg2
import pandas as pd
from importlib import import_module


class PGConnect:
    """
    This class is used to connect and interact with a PostgreSQL database. 
    It provides methods for creating a connection with a PostgreSQL database.
    """

    def __init__(self,
                 database: str,
                 host: str,
                 user: str,
                 password: str,
                 port: str):
        """
        :param database: PG Database name
        :type database: str
        :param host: Host name
        :type host: str
        :param user: Username
        :type user: str
        :param password: Password for user
        :type password: str
        :param port: Database port number
        :type port: str
        """

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
    """
    A class to interact with a PostgreSQL database using SQL queries.
    It uses the `PGConnection` object to handle connections and cursors.
    Also provides methods for creating a table and merging data.
    """
    def __init__(self,
                 db_conn: any,
                 table_name: str,
                 df: pd.DataFrame,
                 merge_column: str,
                 update_columns: list = None):
        """
        :param db_conn: PGConnect instance
        :type db_conn: any
        :param table_name: Table name corresponding to data in dataframe
        :type table_name: str
        :param df: Dataframe
        :type df: pd.DataFrame
        :param merge_column: Primary keys to merge data on
        :type merge_column: str
        :param update_columns: Specific columns to update if updating a row, defaults to all no PK columns
        :type update_columns: list, optional
        """

        self.db_conn = db_conn
        self.table_name = table_name
        self.df = df
        self.merge_column = merge_column
        self.update_columns = update_columns

        self.conn, self.cur = self.db_conn.get_conn()

    def add_updated_date(self):
        """
        Adds an 'Updated' column to the dataframe with current date time values.
        """

        self.df['meta_updated'] = datetime.now()

    def create_table_if_not_exists(self):
        """
        Create table if it doesn't already exist.
        """

        try:
            sql = re.sub('\s+', ' ', open(f'src\database\CT_{self.table_name}.sql', 'r').read())

            self.cur.execute(sql)
            self.conn.commit()
        except:
            raise Exception(f'No CT SQL query found in src\database for table: {self.table_name}')

    def import_schema(self):
        """
        Import schema from JSON object.
        """

        try:
            self.schema = getattr(import_module('schemas'), self.table_name)
            self.ordered_columns = list(self.schema.keys())
        except:
            raise Exception(f'No schema found in schemas.py for table: {self.table_name}')
        
    def enforce_schema(self):
        """
        Check that all columns in a Dataframe are in the correct order.
        """
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
        """
        Execute SQL command to add data to database or update if it already exists.
        """

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
