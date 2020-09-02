import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('DWH tables dropped')

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('DWH tables created')

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
    logging.info('Start of program')

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    logging.info('End of program')

if __name__ == "__main__":
    main()