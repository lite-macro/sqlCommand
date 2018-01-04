import sqlite3
import psycopg2
import pymysql
import toolz.curried
import pandas as pd
from functools import partial
from typing import List
from sqlCommand.utils import Iter, logger, quote, cols_types, quote_join_comma, execute_lite, execute_pg, execute_my
from sqlCommand.utils import conn_lite, conn_pg, conn_my


@toolz.curry
def i_sql(identifier: str, table: str, cols: str, values: str) -> str:
    return 'insert into {0}{1}{0}({2}) values({3})'.format(identifier, table, cols, values)


@toolz.curry
def i_sql_lite(table: str, cols: str, values: str) -> str:
    return i_sql('`', table, cols, values)


@toolz.curry
def i_sql_pg(table: str, cols: str, values: str) -> str:
    return i_sql('"', table, cols, values)


col_lite = quote_join_comma('`')

value_lite = quote_join_comma("'")

col_pg = quote_join_comma('"')

value_pg = quote_join_comma("'")


# @toolz.curry
# def i_lite(conn: conn_lite, table: str, df: pd.DataFrame) -> List[dict]:
#     exceptions = []
#     cur = conn.cursor()
#     li = df.values.tolist()
#     for row in li:
#         try:
#             sql = i_sql_lite(table, col_lite(list(df)), value_lite(row))
#             print(sql)
#             cur.execute(sql)  # do not commit every time because it's very slow
#         except Exception as e:
#             logger.error(e)
#             exceptions.append({'row': row, 'error': e})
#             conn.commit()
#     conn.commit()
#     return exceptions

@toolz.curry
def i_lite(conn: conn_lite, table: str, df: pd.DataFrame) -> List[dict]:
    exceptions = []
    li = df.values.tolist()
    for row in li:
        sql = i_sql_lite(table, col_lite(list(df)), value_lite(row))
        e = execute_lite(conn, sql)
        if e != '':
            exceptions.append({'row': row, 'error': e})
    conn.commit()
    return exceptions


# @toolz.curry
# def i_pg(conn: conn_pg, table: str, df: pd.DataFrame) -> List[dict]:
#     exceptions = []
#     cur = conn.cursor()
#     li = df.values.tolist()
#     for row in li:
#         try:
#             sql = i_sql_pg(table, col_pg(list(df)), value_pg(row))
#             print(sql)
#             cur.execute(sql)  # do not commit every time because it's very slow
#         except Exception as e:
#             logger.error(e)
#             exceptions.append({'row': row, 'error': e})
#             conn.commit()
#     conn.commit()
#     return exceptions


@toolz.curry
def i_pg(conn: conn_pg, table: str, df: pd.DataFrame) -> List:
    exceptions = []
    li = df.values.tolist()
    for row in li:
        sql = i_sql_pg(table, col_pg(list(df)), value_pg(row))
        e = execute_pg(conn, sql)
        if e != '':
            exceptions.append({'row': row, 'error': e})
    conn.commit()
    return exceptions


def insertData(table: str, df, conn, identifier='`', identifier1="'"):
    global errorList
    errorList = []
    cur = conn.cursor()
    l = df.values.tolist()
    for row in l:
        try:
            sql = 'insert into {0}{1}{0}({2}) values({3})'.format(identifier, table, quote_join_comma(identifier, list(df)), quote_join_comma(identifier1, row))
            print(sql)
            cur.execute(sql)  # do not commit every time because it's very slow
        except Exception as e:
            logger.error(e)
            errorList.append(row)
            conn.commit()
    conn.commit()


def insertDataMany(table: str, df, conn, identifier='`', identifier1='?'):
    cur = conn.cursor()
    sql = 'insert into {0}{1}{0}({2}) values({3})'.format(identifier, table, quote_join_comma(identifier, list(df)), ','.join([identifier1] * len(list(df))))
    print(sql)
    try:
        cur.executemany(sql, df.values.tolist())
    except Exception as e:
        logger.error(e)
    conn.commit()


def insertDataMySql(table: str, df, conn, identifier='`', identifier1="'"):
    global errorList
    errorList = []
    cur = conn.cursor()
    l = df.values.tolist()
    for row in l:
        try:
            sql = 'insert into {0}{1}{0}({2}) values({3})'.format(identifier, table, quote_join_comma(identifier, list(df)), quote_join_comma(identifier1, row))
            sql = sql.replace("'nan'", 'NULL')
            print(sql)
            cur.execute(sql)  # do not commit every time because it's very slow
        except Exception as e:
            logger.error(e)
            errorList.append(row)
            conn.commit()
    conn.commit()

# insertDataPostgre = partial(insertData, identifier='"', identifier1='%s')
insertDataPostgre = partial(insertData, identifier='"')
insertDataManyPostgre = partial(insertDataMany, identifier='"', identifier1='%s')


# when UNIQUE constraint failed, executemany in sqlite will still insert unique rows while postgre won't
# def insertData(table: str, df, conn, identifier = '`', identifier1 = '?'):
#     global errorList
#     errorList = []
#     cur = conn.cursor()
#     sql = 'insert into {0}{1}{0}({2}) values({3})'.format(identifier, table, quote_join_comma(identifier, list(df)), ','.join([identifier1]*len(list(df))))
#     l = df.values.tolist()
#     for row in l:
#         try:
#             print(sql, row)
#             cur.execute(sql, row) # do not commit every time because it's very slow
#         except Exception as e:
#             print(e)
#             print(row)
#             errorList.append(row)
#             conn.commit()
#             pass
#     conn.commit()

# def insertData(table: str, df, conn, identifier = '`', identifier1 = "'{}'"):
#     global errorList
#     errorList = []
#     cur = conn.cursor()
#     l = df.values.tolist()
#     for row in l:
#         try:
#             sql = 'insert into {0}{1}{0}({2}) values({3})'.format(identifier, table, quote_join_comma(identifier, list(df)),','.join([identifier1] * len(list(df))))
#             print(sql)
#             sql = sql.format(*row)
#             print(sql)
#             cur.execute(sql) # do not commit every time because it's very slow
#         except Exception as e:
#             print(e)
#             print(row)
#             errorList.append(row)
#             conn.commit()
#             pass
#     conn.commit()
