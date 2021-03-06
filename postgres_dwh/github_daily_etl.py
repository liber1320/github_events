from github_sql_queries import *
import psycopg2
import pandas as pd
import os
import sys 
import glob
import logging
import configparser
import hashlib
from datetime import date
import argparse

def parse_arguments():
    """Function command line parameters."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--path', help = 'path_to_file')
    args = parser.parse_args()
    dict_args = vars(args)
    return dict_args

def process_event_dict_data(cur, conn, filepath):
    df = pd.read_csv(filepath)
    event_dict_data = df[['name']].drop_duplicates()

    dwh_start = date.today()
    dwh_end = date(9999, 12, 31)

    for i, row in event_dict_data.iterrows():
        dict_to_insert = (row[0], dwh_start, dwh_end)
        cur.execute(event_dict_table_insert, dict_to_insert )

    conn.commit()

    logging.info('Events dict data process finished')

def process_actor_data(cur, conn, filepath):
    df = pd.read_parquet(filepath)
    actor_data = df[['actor_id', 'actor_login', 'display_login']].drop_duplicates()

    dwh_start = date.today()
    dwh_end = date(9999, 12, 31)

    for i, row in actor_data.iterrows():
        data_to_insert = (row[0], row[1], row[2], dwh_start, dwh_end)

        cur.execute(actor_login_select, list((str(row[0]), row[1])))
        result = cur.fetchone()

        if not result is None:
            actor_id, actor_login, display_login = result

        if result is None:
            cur.execute(actor_table_insert, data_to_insert)

        if not result is None:
            src_hash = hashlib.md5(str(row[0]).encode('utf-8') +  row[1].encode('utf-8') + row[2].encode('utf-8'))
            dwh_hash = hashlib.md5(str(actor_id).encode('utf-8') + actor_login.encode('utf-8') + display_login.encode('utf-8'))

            if dwh_hash.hexdigest() != src_hash.hexdigest():
               cur.execute(actor_table_update, (dwh_start, str(row[0]), row[1])) 
               cur.execute(actor_table_insert, data_to_insert)
            else:
               continue

    conn.commit()

    logging.info('Actor data process finished')

def process_repo_data(cur, conn, filepath):
    df = pd.read_parquet(filepath)
    repo_data = df[['repo_id', 'repo_name']].drop_duplicates()

    dwh_start = date.today()
    dwh_end = date(9999, 12, 31)

    for i, row in repo_data.iterrows():
        data_to_insert = (row[0], row[1], dwh_start, dwh_end)
        cur.execute(repo_table_insert, data_to_insert)

    conn.commit()

    logging.info('Repo data process finished')

def process_event_data(cur, conn, filepath):
    df = pd.read_parquet(filepath)
    event_data = df[["created_at", "actor_id", "actor_login", "repo_id", "repo_name", "type"]].drop_duplicates()

    dwh_start = date.today()
    dwh_end = date(9999, 12, 31)

    for index, row in event_data.iterrows():

        cur.execute(actor_select, list((str(row.actor_id), row.actor_login)))
        actor = cur.fetchone()
        if actor:
            actor_id = actor
        else:
            actor_id = None

        cur.execute(repo_select, list((str(row.repo_id), row.repo_name)))
        repo = cur.fetchone()
        if repo:
            repo_id = repo
        else:
            repo_id = None

        cur.execute(event_dict_select, (row["type"],))
        event = cur.fetchone()
        if event:
            event_id = event
        else:
            event_id = None

        event_to_insert = (actor_id, repo_id, event_id, row['created_at'], dwh_start, dwh_end)
        cur.execute(event_table_insert, event_to_insert)

    conn.commit()
    logging.info('Events data process finished')

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -  %(levelname)s-  %(message)s')
    logging.info('Start of program')

    args = parse_arguments()
    filepath = args['path']

    config = configparser.ConfigParser()
    config.read('postgres.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(*config['DB_GIT'].values()))
    cur = conn.cursor()

    dict_path = 'data/events.csv'
    if (os.path.exists(dict_path)): 
        process_event_dict_data(cur, conn, dict_path)
    else: 
       logging.info('Dict path not exists.')

    if (filepath != None): 
       if (os.path.exists(filepath)): 
           process_actor_data(cur, conn, filepath)
           process_repo_data(cur, conn, filepath)
           process_event_data(cur, conn, filepath)
       else: 
          logging.info('File path not exists.')
    else: 
       logging.info('File path not specified.')

    conn.close()
    logging.info('End of program')

if __name__ == "__main__":
    main()