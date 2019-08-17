import pandas as pd


def listColumns(table: str, conn):
    sql = 'PRAGMA table_info(`{}`)'.format(table)
    print(sql)
    result = conn.execute(sql)
    l = result.fetchall()
    return [t[1] for t in l]


def getPrimaryKeys(table: str, conn):
    sql = 'PRAGMA table_info(`{}`)'.format(table)
    print(sql)
    df = pd.read_sql_query(sql, conn)
    return list(df[df.pk != 0].name)