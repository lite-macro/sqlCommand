from functools import partial


def renameTable(table: str, table_new, conn, identifier='`'):
    cur = conn.cursor()
    sql = "ALTER TABLE {2}{0}{2} RENAME TO {2}{1}{2};".format(table, table_new, identifier)
    print(sql)
    cur.execute(sql)
    conn.commit()

renameTablePostgre = partial(renameTable, identifier='"')