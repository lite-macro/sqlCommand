import sqlite3
import psycopg2
import pymysql
import toolz.curried
import pandas as pd
from typing import Iterable, List
import logging

# 產生新的logger
logger = logging.Logger('test')

# 定義 handler 輸出 sys.stderr
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# 設定輸出格式
formatter = logging.Formatter('%(asctime)s: %(levelname)-s %(module)s %(lineno)d  %(funcName)s %(message)s')
# handler 設定輸出格式
console.setFormatter(formatter)

logger.addHandler(console)

pd.get_option("display.max_rows")
pd.get_option("display.max_columns")
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 1000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.unicode.east_asian_width', True)

conn_lite = sqlite3.Connection
conn_pg = psycopg2.extensions.connection
conn_my = pymysql.connections.Connection
Iter = Iterable
error = str

@toolz.curry
def join(s: str, it: Iter[str]) -> str:
    return s.join(it)


# mysql use ` while postgresql use "; ' and [ are invalid in postgresql
@toolz.curry
def quote(symbol: any, it: Iter) -> List[str]:
    return ['{0}{1}{0}'.format(symbol, col) for col in it]


@toolz.curry
def quote_join(symbol_join: str, symbol_quote: any, it: Iter) -> str:
    return toolz.compose(join(symbol_join), quote(symbol_quote))(it)


@toolz.curry
def quote_join_comma(symbol: any, it: Iter) -> str:
    return quote_join(', ', symbol, it)


def cols_types(columns: Iter, types: Iter) -> list:
    z = zip(columns, types)
    return ['{} {}'.format(col, type) for col, type in z]


@toolz.curry
def execute(conn, sql: str) -> error:
    err = ''
    cur = conn.cursor()
    print(sql)
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(e)
        err = e
        conn.commit()
    return err


@toolz.curry
def execute_pg(conn: conn_pg, sql: str) -> error:
    return execute(conn, sql)


@toolz.curry
def execute_lite(conn: conn_lite, sql: str) -> error:
    return execute(conn, sql)


@toolz.curry
def execute_my(conn: conn_my, sql: str) -> error:
    return execute(conn, sql)


def changeTypesAndReplace(table: str, inColumns, stringColumns, conn, to_replace=None, value=None):
    try:
        df = selectAll(table, conn).replace(to_replace, value)
        df[inColumns] = df[inColumns].astype(int)
        df[stringColumns] = df[stringColumns].astype(str)
        floatColumns = [col for col in list(df) if col not in inColumns + stringColumns]
        df[floatColumns] = df[floatColumns].astype(float)
        primary_keys = getPrimaryKeys(table, conn)
        columns = list(df)
        table_old = table + '0'
        renameTable(table, table_old, conn)
        createTable(table, columns, primary_keys, conn)
        insertData(table, df, conn)
        dropTable(table_old, conn)
    except Exception as e:
        logger.error(e)
