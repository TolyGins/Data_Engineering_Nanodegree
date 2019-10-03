import pytest
import configparser

from etl import  sql_exec, create_table_queries

class DbConnect():
    import psycopg2
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

def test_CanInstantiateSqlExec():
    query = ['SELECT COUNT(*) FROM victory.raw_events;']
    sql_exec(db.cur, db.conn, query)

def test_CanRunCreateTables():
    query = create_table_queries
    sql_exec(db.cur, db.conn, query)

def test_rows_in_raw_table():
    query = 'SELECT COUNT(*) FROM victory.raw_events'
    db.cur.execute(query)
    rows = db.cur.fetchone()[0]
    print (f' Number of rows in raw_events is {rows}')
    db.conn.close()
    assert rows > 0 

db = DbConnect()