import os
import sys
import json
import pymysql

import pandas as pd

from functools import wraps
from sqlalchemy import create_engine

import pymysql.cursors

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from modules.util.logger import Logger

class DBM:
    def __init__(self, db_name):
        self.db = db_name
        self.logger = Logger().get_logger(module_name='modules.data.dbm')
        
        with open('./config/db/connection_configs.json', 'r') as config:
            self.connection_config = json.load(config)
            
        with open('./config/db/table_configs.json', 'r', encoding='utf8') as config:
            self.tbl_config = json.load(config)
        
        self.connection_params = {
            'host': self.connection_config['host'],
            'port': int(self.connection_config['port']),
            'user': self.connection_config['user'],
            'password': self.connection_config['password'],
            'database': self.db
        }
        
        self.engine = create_engine(f"mysql+pymysql://{self.connection_params['user']}:{self.connection_params['password']}@{self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}")
        
    def db_operation(create_db=False):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                try:
                    if create_db:
                        conn = pymysql.connect(**self.connection_params)
                        with conn.cursor() as cursor:
                            cursor.execute(f"SHOW DATABASES LIKE %s", (self.db,))
                            if not cursor.fetchone():
                                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
                                conn.commit()
                                self.logger.info(f"Create database {self.db}.")
                        conn.close()
                    
                    self.conn = pymysql.connect(**self.connection_params, db=self.db)
                    self.cursor = self.conn.cursor()
                    
                    # self.logger.info(f"Connect to database {self.db}.")
                    
                    result = func(self, *args, **kwargs)
                    return result
                except Exception as e:
                    self.logger.error(f"Error connecting to database {self.db} : {str(e)}.")
                    raise
                finally:
                    if hasattr(self, 'cursor') and self.cursor:
                        self.cursor.close()
                    if hasattr(self, 'conn') and self.conn.open:
                        self.conn.close()
            return wrapper
        return decorator
    
    @db_operation(create_db=True)
    def create_table(self, tbl_name, query=None):
        self.cursor.execute(f"SHOW TABLES LIKE %s", (tbl_name,))
        if not self.cursor.fetchone():
            cols_schemas = ', '.join([f"{col} {value}" for col, value in self.tbl_config[tbl_name]['schemas'].items()])
            if query is None:
                query = f"CREATE TABLE {tbl_name} ({cols_schemas})"
            else:
                query = query
            try:
                self.cursor.execute(query)
                self.conn.commit()
                self.logger.info(f"Create table {tbl_name}.")
            except Exception as e:
                self.logger.error(f"Error creating table {tbl_name} : {str(e)}.")
        else:
            self.logger.info(f"Table({tbl_name}) is already exists.")
            
    @db_operation(create_db=False)
    def set_pk(self, tbl_name, pk_col_name, const_name, query=None):
        if query is None:
            query = f"ALTER TABLE {tbl_name} ADD CONSTRAINT {const_name} PRIMARY KEY ({pk_col_name})"
        else:
            query = query
        try:
            self.cursor.execute(query)
            self.conn.commit()
            self.logger.info(f"Set primary key for ({tbl_name} - {pk_col_name}).")
        except Exception as e:
            self.logger.error(f"Error setting primary key for ({tbl_name} - {pk_col_name}) : {str(e)}.")
        
    @db_operation(create_db=False)
    def set_fk(self, fk_tbl_name, pk_tbl_name, fk_col_name, pk_col_name, const_name, query=None):
        if query is None:
            query = f"ALTER TABLE {fk_tbl_name} ADD CONSTRAINT {const_name} FOREIGN KEY ({fk_col_name}) REFERENCES {pk_tbl_name} ({pk_col_name})"
        else:
            query = query
        try:
            self.cursor.execute(query)
            self.conn.commit()
            self.logger.info(f"Set foreign key for ({fk_tbl_name} - {fk_col_name}) referenced by ({pk_tbl_name} - {pk_col_name}).")
        except Exception as e:
            self.logger.error(f"Error setting foreign key for ({fk_tbl_name} - {fk_col_name}) : {str(e)}.")
        
    @db_operation(create_db=False)
    def insert_data(self, tbl_name, data_list, query=None):
        cols_list = self.tbl_config[tbl_name]['list']
        cols = ', '.join(cols_list)
        placeholders = ', '.join(['%s'] * len(cols_list))
        if query is None:
            query = f"INSERT INTO {tbl_name} ({cols}) VALUES ({placeholders})"
        else:
            query = query
        try:
            self.cursor.executemany(query, data_list)
            self.conn.commit()
        except Exception as e:
            self.logger.error(f"Error inserting data to {tbl_name} : {str(e)}.")
            
    @db_operation(create_db=False)
    def import_data(self, tbl_name, call_cols, limit=1000000, dates=None, query=None):
        if call_cols:
            call_cols = ", ".join(call_cols)
        if query is None:
            query = f"SELECT {call_cols} FROM {tbl_name} LIMIT {limit}"
        else:
            query = query
        try:
            data = pd.read_sql(query, self.engine, parse_dates=dates)
            self.logger.info(f"Import {len(data)} rows from {tbl_name}.")
            
            return data
        except Exception as e:
            self.logger.error(f"Error importing data from {tbl_name} : {str(e)}.")
            return None
        
if __name__ == '__main__':
    dbm = DBM(db_name='atamDB')
    
    tbl_list = ['trade', 'total_district_code', 'district_code']
    for tbl in tbl_list:
        dbm.create_table(tbl)
    dbm.set_pk(tbl_name='district_code', pk_col_name='region_code', const_name='pk_district_code')
    dbm.set_fk(fk_tbl_name='trade', pk_tbl_name='district_code', fk_col_name='region_code', pk_col_name='region_code', const_name='fk_trade')
    dbm.set_fk(fk_tbl_name='total_district_code', pk_tbl_name='district_code', fk_col_name='region_code', pk_col_name='region_code', const_name='fk_total_district_code')
    
    district_code = pd.read_csv('./docs/district_code_src/district_code.csv', encoding='cp949')
    district_code = district_code.where(pd.notnull(district_code), None)
    district_code = district_code.values.tolist()

    total_district_code = pd.read_csv('./docs/district_code_src/total_district_code.csv', encoding='cp949')
    total_district_code = total_district_code.where((pd.notnull(total_district_code)), None)
    total_district_code = total_district_code.values.tolist()
    dbm.insert_data('district_code', district_code)
    dbm.insert_data('total_district_code', total_district_code)