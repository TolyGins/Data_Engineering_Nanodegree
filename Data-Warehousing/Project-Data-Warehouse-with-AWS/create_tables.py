import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """This function executes a list of drop table commands found in sql_queries module
    
    Args:
        cur: This is the db cursor from psychopg2
        param2: This is the db connection from psychopg2

    Returns:
        The return in this function is actually commiting the sql execution to the db
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """This function executes a list of create table commands found in sql_queries module
    
    Args:
        cur: This is the db cursor from psychopg2
        param2: This is the db connection from psychopg2

    Returns:
        The return in this function is actually commiting the sql execution to the db
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function sets up the db connection to redshift and calls the previous two functions """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()