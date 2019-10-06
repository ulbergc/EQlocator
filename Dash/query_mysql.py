'''
Code to return earthquake and station information from MySQL database

Modeled after code from Chrissy Qiu: https://github.com/chrissyqyc9

Filename: query_mysql.py
Cohort: Insight Data Engineering SEA '19C
Name: Carl Ulberg
'''


import mysql.connector
import pandas as pd
import os


def ReadConnector(db_name):
    conn = mysql.connector.connect(
        host=os.environ['MYSQL_HOST'],
        database=db_name,
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'],
        use_pure=True
    )
    return conn


def Query(sql, conn):
    cur = conn.cursor(buffered=True)
    df = pd.read_sql(sql, conn)
    cur.close()
    return df


def ReadDisconnect(conn):
    conn.close()


def get_stations(conn):
    sql = 'SELECT * FROM tmp5.sta_month'
    df = Query(sql, conn)
    return df


def get_locations(date, method, conn):
    if method == 0:
        table = 'table7'
    elif method == 1:
        table = 'table6'
    elif method == 2:
        table = 'table4'
    else:
        print('Method {} does not exist'.format(method))

    datestr1 = '{}00000000'.format(date)
    datestr2 = '{}00000000'.format(date+1)
#    sql = 'select * from {} \
#            where Event_id between {} and {} \
#            and rand() <= .1;' \
#        .format(table, datestr1, datestr2)

    sql = 'select * from {} where Event_id between {} and {};' \
        .format(table, datestr1, datestr2)

    df = Query(sql, conn)
    return df
