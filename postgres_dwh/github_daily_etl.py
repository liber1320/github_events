from github_sql_queries import *
import psycopg2
import pandas as pd
import os
import sys 
import glob
import logging
import configparser
from datetime import date

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
        actor_login = cur.fetchone()

        if actor_login is None:
            cur.execute(actor_table_insert, data_to_insert)
        elif actor_login[0] != row[2]:
            cur.execute(actor_table_update, (dwh_start, row[0], row[1]))
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

    try:
       filepath = sys.argv[1] 
    except:
       logging.info('Improper path.')
    config = configparser.ConfigParser()
    config.read('postgres.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(*config['DB_GIT'].values()))
    cur = conn.cursor()

    dict_path = 'data/events.csv'
    if (os.path.exists(dict_path)): 
        process_event_dict_data(cur, conn, dict_path)
    else: 
       logging.info('Dict path not exists.')

    if (os.path.exists(filepath)): 
        process_actor_data(cur, conn, filepath)
        process_repo_data(cur, conn, filepath)
        process_event_data(cur, conn, filepath)
    else: 
       logging.info('File path not exists.')

    conn.close()
    logging.info('End of program')

if __name__ == "__main__":
    main()