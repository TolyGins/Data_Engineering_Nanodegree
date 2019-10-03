import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries, schemas_create, insert_statements
from Copy_From_S3 import copy_comands


def sql_exec(cur, conn, query_list):
    """This function executes a list of SQL commands found in sql_queries module
    
    Args:
        cur: This is the db cursor from psychopg2
        conn: This is the db connection from psychopg2
        query_list : This is the list of queries we are passing
    Returns:
        The return in this function is actually commiting the sql execution to the db """
    for query in query_list:
        print (query)
        cur.execute(query)
        conn.commit()


def main():
    """This function sets up the db connection to redshift and calls the previous two functions """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # sql_exec(cur, conn, schemas_create)
    # sql_exec(cur, conn, drop_table_queries)
    sql_exec(cur, conn, create_table_queries)
    #sql_exec(cur, conn, copy_comands)
    #sql_exec (cur, conn, insert_statements)

    conn.close()


if __name__ == "__main__":
    main()
