from github_sql_queries import create_table_queries, drop_table_queries
import psycopg2
import logging
import configparser

def create_database():
    """
    Creates and connects to the gitdb
    Returns the connection and cursor to gitdb
    """
    config = configparser.ConfigParser()
    config.read('postgres.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(*config['DB_MAIN'].values()))
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS gitdb_")
    cur.execute("CREATE DATABASE gitdb_ WITH ENCODING 'utf8' TEMPLATE template0")
    logging.info('Database ready')
    conn.close()    

    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(*config['DB_GIT'].values()))
    cur = conn.cursor()
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Tables dropped')

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Tables created')

def main():
    """
    - Drops (if exists) and Creates database. 
    - Establishes connection with the database and gets cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -  %(levelname)s-  %(message)s')
    logging.info('Start of program')

    cur, conn = create_database()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

    logging.info('End of program')

if __name__ == "__main__":
    main()