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
def execute(cur, sql: str) -> None:
    print(sql)
    cur.execute(sql)


@toolz.curry
def execute_pg(cur: psycopg2.extensions.cursor, sql: str) -> None:
    return execute(cur, sql)


@toolz.curry
def execute_lite(cur: sqlite3.Cursor, sql: str) -> None:
    return execute(cur, sql)


@toolz.curry
def execute_my(cur: pymysql.cursors.Cursor, sql: str) -> None:
    return execute(cur, sql)

