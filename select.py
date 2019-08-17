import sqlite3
import psycopg2
import pymysql
import pandas as pd
import toolz.curried
from typing import Iterable
from functools import partial
from sqlCommand.utils import join, logger, quote, cols_types, quote_join_comma, execute_lite, execute_pg, execute_my
from sqlCommand.utils import conn_lite, conn_pg, conn_my
from astype import as_type_dict


@toolz.curry
def s_dist_lite(conn, table: str, cols: Iterable) -> pd.DataFrame:
    sql = 'SELECT distinct {} FROM "{}"'.format(quote_join_comma("`", cols), table)
    return pd.read_sql_query(sql, conn)


@toolz.curry
def s_dist_pg(conn: conn_pg, table: str, cols: Iterable) -> pd.DataFrame:
    sql = 'SELECT distinct {} FROM "{}"'.format(quote_join_comma('"', cols), table)
    return pd.read_sql_query(sql, conn)


@toolz.curry
def s_where_lite(conn, table, row) -> pd.DataFrame:
    sql = 'SELECT * FROM `{}` where {}'.format(table, join(' and ', ["`{0}`='{1}'".format(key, value) for key, value in row.items()]))
    print(sql)
    return pd.read_sql_query(sql, conn)


@toolz.curry
def s_where_with_type_lite(cur: sqlite3.Cursor, table: str, dtypes: dict, row: dict) -> pd.DataFrame:
    # sqlite3 also needs type to select data correctly, which type is correct needs try and error, sometimes int or str can't not be distinquished explicitly
    sql = 'SELECT * FROM `{}` where {}'.format(table, join(' and ', ["{}=?".format(key) for key in row.keys()]))
    print(sql)
    row = as_type_dict(dtypes, row)
    print(row)
    cur.execute(sql, tuple(row.values()))
    data = cur.fetchall()
    cur.execute("SELECT name FROM pragma_table_info('{}')".format(table))
    cols = cur.fetchall()
    return pd.DataFrame(data, columns = [col[0] for col in cols])


@toolz.curry
def s_all_lite(conn: conn_lite, table: str) -> pd.DataFrame:
    sql = "SELECT * from `{}`".format(table)
    print(sql)
    return pd.read_sql_query(sql, conn)


@toolz.curry
def s_all_pg(conn: conn_pg, table: str) -> pd.DataFrame:
    sql = 'SELECT * from "{}"'.format(table)
    print(sql)
    return pd.read_sql_query(sql, conn)


@toolz.curry
def s_all_my(conn: conn_my, table: str) -> pd.DataFrame:
    sql = "SELECT * from `{}`".format(table)
    print(sql)
    return pd.read_sql_query(sql, conn)


def selectAll(table: str, conn, identifier='`'):
    sql = "SELECT * from {1}{0}{1} ;".format(table, identifier)
    print(sql)
    return pd.read_sql_query(sql, conn)


selectAllPostgre = partial(selectAll, identifier='"')

#----select DISTINCT ----
def selectDistinct(columns, table: str, conn, identifier = '`'):
    sql = "SELECT DISTINCT {0} from {2}{1}{2} ;".format(quote_join_comma(identifier, columns), table, identifier)
    print(sql)
    return pd.read_sql_query(sql, conn)

selectDistinctPostgre = partial(selectDistinct, identifier='"')

#----select last row ----
def selectLast(columns, table: str, conn, identifier = '`'):
    sql = "SELECT * from {2}{1}{2} ORDER BY {0} DESC LIMIT 1;".format(quote_join_comma(identifier, columns), table, identifier)
    print(sql)
    return pd.read_sql_query(sql, conn)
