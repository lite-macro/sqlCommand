import sqlite3
import psycopg2
import pymysql
import toolz.curried
from typing import Iterable
from functools import partial
from sqlCommand.utils import logger, quote, cols_types, quote_join_comma, execute_lite, execute_pg, execute_my


#----delete data----

def deleteFrom(table: str, conn, identifier='`'):
    cur = conn.cursor()
    sql = 'DELETE FROM {1}{0}{1}'.format(table, identifier)
    print(sql)
    cur.execute(sql)
    conn.commit()

def dropTable(table: str, conn, identifier='`'):
    cur = conn.cursor()
    sql = 'DROP TABLE IF EXISTS {1}{0}{1}'.format(table, identifier)
    print(sql)
    cur.execute(sql)
    conn.commit()

deleteFromPostgre = partial(deleteFrom, identifier='"')
dropTablePostgre = partial(dropTable, identifier='"')