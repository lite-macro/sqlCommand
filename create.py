import sqlite3
import psycopg2
import pymysql
import toolz.curried
from sqlCommand.utils import logger, quote, cols_types, quote_join_comma, execute_lite, execute_pg, execute_my
from sqlCommand.utils import Iter, conn_lite, conn_pg, conn_my


@toolz.curry
def ct_sql(identifier, table: str, columns: Iter[str], primary_keys: Iter[str]) -> str:
    columns_join = ', '.join(columns)
    primary_keys_join = ', '.join(primary_keys)
    sql = 'create table {0}{1}{0} ({2}, PRIMARY KEY({3}))'.format(identifier, table, columns_join, primary_keys_join)
    return sql


@toolz.curry
def ct_lite_sql(table: str, columns: Iter, primary_keys: Iter) -> str:
    return ct_sql('`', table, columns, primary_keys)


@toolz.curry
def ct_pg_sql(table: str, columns: Iter, field_types: Iter, primary_keys: Iter) -> str:
    z = zip(columns, field_types)
    return ct_sql('"', table, ['{} {}'.format(col, type) for col, type in z], primary_keys)


@toolz.curry
def ct_my_sql(table: str, columns: Iter, field_types: Iter, primary_keys: Iter) -> str:
    z = zip(columns, field_types)
    return ct_sql('`', table, ['{} {}'.format(col, type) for col, type in z], primary_keys)


@toolz.curry
def ct_lite(conn: conn_lite, table: str, columns: Iter, primary_keys: Iter) -> None:
    execute_lite(conn, ct_pg_sql(table, columns, primary_keys))
    conn.commit()


@toolz.curry
def ct_pg(conn: conn_pg, table: str, columns: Iter, field_types: Iter, primary_keys: Iter) -> None:
    execute_pg(conn, ct_pg_sql(table, columns, field_types, primary_keys))
    conn.commit()


@toolz.curry
def ct_my(conn: conn_my, table: str, columns: Iter, field_types: Iter, primary_keys: Iter) -> None:
    execute_my(conn, ct_pg_sql(table, columns, field_types, primary_keys))
    conn.commit()


def createTablePostgre(table: str, columns, field_types, primary_keys, conn, identifier='"'):
    cur = conn.cursor()
    columnsQuote = quote(identifier, columns)
    columns_join = ', '.join(cols_types(columnsQuote, field_types))
    primary_keys_join = quote_join_comma(identifier, primary_keys)
    sql = 'create table "{}" ({}, PRIMARY KEY({}))'.format(table, columns_join, primary_keys_join)
    print(sql)
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(e)
        pass
    conn.commit()


def createTableMySql(table: str, columns, field_types, primary_keys, conn, identifier='`'):
    cur = conn.cursor()
    columnsQuote = quote(identifier, columns)
    columns_join = ', '.join(cols_types(columnsQuote, field_types))
    primary_keys_join = quote_join_comma(identifier, primary_keys)
    sql = 'create table `{}` ({}, PRIMARY KEY({}))'.format(table, columns_join, primary_keys_join)
    print(sql)
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(e)
        pass
    conn.commit()

def createTable(table: str, columns, primary_keys, conn, identifier = '`'):
    columns_join = quote_join_comma(identifier, columns)
    primary_keys_join = quote_join_comma(identifier, primary_keys)
    sql = 'create table "{}" ({}, PRIMARY KEY({}))'.format(table, columns_join, primary_keys_join)
    print(sql)
    try:
        conn.execute(sql)
    except Exception as e:
        logger.error(e)
        pass
    conn.commit()