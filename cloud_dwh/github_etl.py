import configparser
import psycopg2
import sql_queries as q
import logging

def create_tables(cur, conn):
    for query in q.create_staging_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Staging tables created')


def load_staging_tables(cur, conn):
    for query in q.copy_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Staging tables copied')


def upsert_staging_tables(cur, conn):
    for query in q.insert_staging_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Staging tables loaded')


def delete_staging_tables(cur, conn):
    for query in q.delete_staging_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Staging tables updated')

def insert_tables(cur, conn):
    for query in q.insert_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Main tables loaded')

def drop_tables(cur, conn):
    for query in q.drop_staging_table_queries:
        cur.execute(query)
        conn.commit()
    logging.info('Staging tables dropped')

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
    logging.info('Start of program')

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #drop_tables(cur, conn)
    create_tables(cur, conn)
    load_staging_tables(cur, conn)
    upsert_staging_tables(cur, conn)
    delete_staging_tables(cur, conn)
    insert_tables(cur, conn)
    drop_tables(cur, conn)

    conn.close()
    logging.info('End of program')

if __name__ == "__main__":
    main()