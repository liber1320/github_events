import psycopg2
from github_sql_queries import create_table_queries, drop_table_queries
import logging

def create_database():
    """
    Creates and connects to the gitdb
    Returns the connection and cursor to gitdb
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password==student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    #create gitdb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS gitdb")
    cur.execute("CREATE DATABASE gitdb WITH ENCODING 'utf8' TEMPLATE template0")
    conn.close()    
    
    # connect to gitdb database
    conn = psycopg2.connect("host=127.0.0.1 dbname=gitdb user=postgres password==student")")
    cur = conn.cursor()
    return cur, conn

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    - Establishes connection with the sparkify database and gets cursor to it.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -  %(levelname)s-  %(message)s')
    logging.info('Start of program')

    cur, conn = create_database()
    drop_tables(cur, conn)
    logging.info('Tables dropped')
    create_tables(cur, conn)
    logging.info('Tables created')
    conn.close()

if __name__ == "__main__":
    main()