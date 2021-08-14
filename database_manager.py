import os

import pandas as pd

from json import loads
from os.path import (
    join,
    exists,
    abspath,
    dirname,
)
from pymysql import (
    cursors,
    connect,
)

from helper import (
    get_str,
)

class DatabaseManager:
    file = ''
    __conn = None
    result_ = None
    
    def __init__(self, file='db_creds.json'):
        self.file = file
        pwd = dirname(abspath(__file__))
        path = join(pwd, file)
        if not exists(path):
            raise FileNotFoundError('File', path, 'not found')
        keys = [
            'user',
            'password',
            'host',
            'port',
        ]
        f = open(path)
        creds = loads(f.read())
        '''
        Read credential from json
        Check if all the necessary keys are in the json
        '''
        for key in keys:
            if key not in creds:
                raise KeyError('Key', key, 'not found in credential file', path)
        self.__conn = connect(
            user=creds['user'],
            password=creds['password'],
            host=creds['host'],
            port=creds['port'],
            charset='utf8mb4',
            db=creds['db'],
            cursorclass=cursors.DictCursor,
        )

    def is_open(self):
        if self.__conn != None:
            return self.__conn.open
    
    def get_df_from_query(self, query):
        return pd.read_sql(query, self.__conn)

    def get_connection(self):
        return self.__conn

    def form_query_select(
        self, 
        table_name='', 
        columns=[], 
        where_fields=[],
        where_values=[], 
        between_fields=[], 
        between_first=[], 
        between_second=[],
        group_by=None,
    ):
        sql = 'SELECT '
        if len(columns) < 1:
            sql += '* '
        else:
            sql += columns[0]
            for col in columns:
                sql += ', '
                sql += col
        sql += ' FROM '
        sql += table_name
        if len(where_fields) > 0:
            if len(where_fields) != len(where_values):
                raise ValueError('Count mismatch. Columns:', len(where_fields), 'Value:', len(where_values))
            sql += ' WHERE '
            sql += str(where_fields[0])
            sql += ' = '
            sql += str(where_values[0])
            for field, value in zip(where_fields[1:], where_values[1:]):
                sql += ' AND '
                sql += str(field)
                sql += ' = '
                sql += str(value)
        if (len(between_fields)) > 0:
            if (len(between_fields)) != len(between_first) or len(between_first) != len(between_second):
                raise ValueError(
                    'Length mismatch. Between fields',
                    len(between_fields), 
                    'Between first values', 
                    len(between_first),
                    'Between second values',
                    len(between_second),
                )
            start = 1
            if (len(where_fields)) > 0:
                start = 0
            if start == 1:
                sql += ' WHERE '
                sql += between_fields[0]
                sql += ' BETWEEN '
                sql += str(between_first)
                sql += ' AND '
                sql += str(between_second)
            for (field, first, second) in zip(between_fields, between_first, between_second):
                sql += ' AND '
                sql += field
                sql += ' BETWEEN '
                sql += first
                sql += ' AND '
                sql += second
        if len(group_by) > 0:
            sql += ' GROUP BY '
            sql += group_by
        sql += ';'
        return sql

    def form_insert_sql(self, table_name='', fields=[], ignore_duplicates=False, replace_duplicates=False):
        sql = 'INSERT INTO '
        if replace_duplicates:
            sql = "REPLACE INTO "
        elif ignore_duplicates:
            sql = "INSERT IGNORE INTO "
        sql += table_name
        if len(fields) < 1:
            raise ValueError('Empty field list')
        '''
        Use "field_name" instead of 'field_name'
        Otherwise SQL will confuse the field names with others
        '''
        sql += ' ('
        # sql += '`'
        sql += str(fields[0])
        # sql += '`'
        for field in fields[1:]:
            sql += ', '
            # sql += '`'
            sql += str(field)
            # sql += '`'
        sql += ') VALUES (%'
        sql += 's'
        for field in fields[1:]:
            sql += ', %'
            sql += 's'
        sql += ')'
        
        return sql

    def insert_many(self, table_name, fields, values, ignore_duplicates=False, replace_duplicates=False):
        sql = self.form_insert_sql(
            table_name, 
            fields,
            ignore_duplicates=ignore_duplicates,
            replace_duplicates=replace_duplicates
        )
        # print(sql)
        rows = []
        '''
        DO NOT EXECUTE A QUERY PER ROW
        Instead, form a query and run the whole query at once
        Form a list of tuples
        Every entry is numpy.float64, so convert it to float
        Otherwise pymysql does not recognize type
        '''
        for row in values:
            cur = ()
            for a in row:
                cur += (get_str(a),)
            rows.append(cur)
            # print(cur)
        print(rows[:5])
        '''
        Use executemany options of pymysql cursor
        '''
        try:
            cursor = self.__conn.cursor()
            cursor.executemany(sql, rows)
            self.__conn.commit()
        except:
            '''
            If writing in database fails, then write sql to file
            '''
            #f = open('query.sql', 'w')
            #f.write(sql)
            #f.close()
            raise ConnectionRefusedError('Can not write to database')

    def execute(self, sql):
        try:
            cursor = self.__conn.cursor()
            # print('sql query: ',sql)
            cursor.execute(sql)
            self.__conn.commit()
            self.result_ = cursor.fetchall()
        finally:
            pass

    def close(self):
        try:
            self.__conn.close()
        except:
            raise ConnectionError('Can not close connection')
