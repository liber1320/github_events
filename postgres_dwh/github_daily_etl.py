import os
import sys 
import glob
import psycopg2
import pandas as pd
from github_sql_queries import *
import logging

def process_event_dict_data(cur, conn, filepath):
    df = pd.read_csv(filepath)
    event_dict_data = df[['name']].drop_duplicates()

    for i, row in event_dict_data.iterrows():
        cur.execute(event_dict_table_insert, list(row))
    conn.commit()
    logging.info('Events dict data inserted')

def process_actor_data(cur, conn, filepath):
    df = pd.read_parquet(filepath)
    actor_data = df[['actor_id','actor_login']].drop_duplicates()

    for i, row in actor_data.iterrows():
        cur.execute(actor_table_insert, list(row))
    conn.commit()
    logging.info('Actor data inserted')

def process_repo_data(cur, conn, filepath):
    df = pd.read_parquet(filepath)
    repo_data = df[['repo_id','repo_name']].drop_duplicates()

    for i, row in repo_data.iterrows():
        cur.execute(repo_table_insert, list(row))
    conn.commit()
    logging.info('Repo data inserted')

def process_event_data(cur, conn, filepath):
    df = pd.read_parquet(filepath)
    event_data = df[["created_at", "actor_id", "actor_login", "repo_id", "repo_name", "type"]].drop_duplicates()

    for index, row in df.iterrows():

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

        event_to_insert = (actor_id, repo_id, event_id, row['created_at'])
        cur.execute(event_table_insert, event_to_insert)
    conn.commit()
    logging.info('Event data inserted')

def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s -  %(levelname)s-  %(message)s')
    logging.info('Start of program')

## path to file in current directory for example 'df_2020-07-01.parquet'
    try:
       filepath = sys.argv[1] 
    except:
       logging.info('Improper path.')

    conn = psycopg2.connect("host=127.0.0.1 dbname=gitdb user=postgres password==student")")
    cur = conn.cursor()

    dict_path = 'data/events.csv'

    if (os.path.exists(filepath)): 
        process_event_dict_data(cur, conn, dict_path)
        process_actor_data(cur, conn, filepath)
        process_repo_data(cur, conn, filepath)
        process_event_data(cur, conn, filepath)
    else: 
       logging.info('Path not exists.')

    conn.close()
    logging.info('End of program')

if __name__ == "__main__":
    main()