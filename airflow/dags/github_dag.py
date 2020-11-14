from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import PostgresOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.git_plugin import StageToRedshiftOperator
from airflow.operators.git_plugin import (LoadDimensionOperator, LoadDictOperator, LoadFactOperator)
from airflow.models import Variable
import airflow.hooks.S3_hook
from helpers import SqlQueries
import configparser
import urllib.request
import boto3
import datetime
import logging
import sys
import os

config = configparser.ConfigParser()
config.read('airflow_param.cfg')
home_dir = os.getcwd()

def download_data(*args, **kwargs):

        execution_date = kwargs["execution_date"]
        year = execution_date.year
        month = execution_date.month
        day = execution_date.day-1
        date = '{}-{}-{}'.format(year, month, str(day).zfill(2))

        cur_dir = os.getcwd()
        wd = '{}/{}'.format(cur_dir, date)
        if not os.path.isdir(wd):
                os.mkdir(wd)
        os.chdir(wd)

        opener = urllib.request.URLopener()
        opener.addheader('User-Agent', 'whatever')
        for i in range(0, 24):
                url = 'http://data.gharchive.org/{}-{}.json.gz'.format(date, i)
                filename = '{}-{}.gz'.format(date, i)
                try:
                        filename, headers = opener.retrieve(url, filename)
                except Exception as exc:
                        logging.info('There was a problem for day %s hour %s: %s ' % (day, i, exc))

        Variable.set('year', year)
        Variable.set('month', month)
        Variable.set('day', day)

        logging.info('******* Data downloading ended. *******')

def upload_file_to_S3_with_hook(*args, **kwargs):

    execution_date = kwargs["execution_date"]
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day-1
    date = '{}-{}-{}'.format(year, month, str(day).zfill(2))

    local_directory = home_dir + '/git_' + date + '.parquet'
    destination = 'git_' + date + '.parquet'

    s3_conn = config.get("S3", "CONNECTION")
    s3_bucket = config.get("S3", "BUCKET")
    hook = airflow.hooks.S3_hook.S3Hook(s3_conn)

    for root, dirs, files in os.walk(local_directory):
        for filename in files:
           local_path = os.path.join(root, filename)
           relative_path = os.path.relpath(local_path, local_directory)
           s3_path = os.path.join(destination, relative_path)
           hook.load_file(local_path, s3_path, s3_bucket) 

dag = DAG('Git_pipeline',
          schedule_interval = '@daily',
          start_date = datetime.datetime.now() - datetime.timedelta(days=1),
          description = 'Pipeline to download, process and load git data from web to redshift',
          max_active_runs = 1
          )

start_operator = DummyOperator(
          task_id = 'begin_execution', 
          dag = dag
          )

download_git_data = PythonOperator(
          task_id = "download_git_data",
          python_callable = download_data,
          dag = dag,
          provide_context = True
          )

spark_config = {
          'conn_id': config.get('HOST', 'SPARK_CONN'),
          'application': config.get('HOST', 'SPARK_APP')
          }

spark_process= SparkSubmitOperator(
          task_id = "spark_submit",
          dag = dag,
          **spark_config
          )

del_json_task = BashOperator(
          task_id = "delete_old_data",
          bash_command = 'rm -r '+ home_dir + '/"{{ (execution_date - macros.timedelta(days=3)).strftime("%Y-%m-%d") }}"',
          )

del_crc_task = BashOperator(
          task_id = "delete_crc_data",
          bash_command = 'find '+ home_dir + '/git_{{ (execution_date - macros.timedelta(days=2)).strftime("%Y-%m-%d") }}.parquet/ -name "*.crc" -exec rm \'{}\' \;',
          )

del_suc_task = BashOperator(
          task_id = "delete_success_data",
          bash_command = 'rm -r ' + home_dir + '/git_{{ (execution_date - macros.timedelta(days=2)).strftime("%Y-%m-%d") }}.parquet/_SUCCESS',
          )

upload_to_S3_task = PythonOperator(
          task_id = 'upload_parquet_to_S3',
          python_callable = upload_file_to_S3_with_hook,
          dag = dag,
          provide_context = True)

create_main_tables = PostgresOperator(
          task_id = "create_main_tables",
          dag = dag,
          postgres_conn_id = config.get("REDSHIFT", "CONNECTION"),
          sql = "create_main_tables.sql"
          )

create_stage_tables = PostgresOperator(
          task_id = "create_stage_tables",
          dag = dag,
          postgres_conn_id = config.get("REDSHIFT", "CONNECTION"),
          sql = "create_stage_tables.sql"
          )

stage_events_dict_to_redshift = StageToRedshiftOperator(
          task_id = 'staging_events_dict_copy',
          redshift_conn_id = config.get("REDSHIFT", "CONNECTION"),
          aws_credentials_id = config.get("AWS", "CREDENTIALS"),
          table = 'events_dict_staging',
          s3_bucket = config.get("S3", "BUCKET"),
          s3_key = 'events.csv',
          region = 'us-east-1',
          format = 'csv',
          dag = dag
          )

year = Variable.get('year')
month =  Variable.get('month')
day = int(Variable.get('day'))
date = '{}-{}-{}'.format(year, month, str(day).zfill(2))
key =  'git_' + date + '.parquet'

stage_events_to_redshift = StageToRedshiftOperator(
          task_id = 'staging_events_copy',
          redshift_conn_id = config.get("REDSHIFT", "CONNECTION"),
          aws_credentials_id = config.get("AWS", "CREDENTIALS"),
          table = 'events_staging',
          s3_bucket = config.get("S3", "BUCKET"),
          s3_key = key,
          region = 'us-east-1',
          format = 'parquet',
          dag = dag
          )

load_actor_dimension_table = LoadDimensionOperator(
          task_id = 'load_actor_dim_table',
          redshift_conn_id = config.get("REDSHIFT", "CONNECTION"),
          table = 'actors',
          select_sql = SqlQueries.actor_staging_table_insert,
          key1 = 'actor_id',
          key2 = 'actor_login',
          dag = dag
          )

load_repo_dimension_table = LoadDimensionOperator(
          task_id = 'load_repo_dim_table',
          redshift_conn_id = config.get("REDSHIFT", "CONNECTION"),
          table = 'repos',
          select_sql = SqlQueries.repo_staging_table_insert,
          key1 = 'repo_id',
          key2 = 'repo_name',
          dag = dag
          )

load_dict_dimension_table = LoadDictOperator(
          task_id = 'load_dict_dim_table',
          redshift_conn_id = config.get("REDSHIFT", "CONNECTION"),
          table = 'events_dict',
          select_sql = SqlQueries.event_dict_staging_table_insert,
          key = 'event',
          dag = dag
          )

load_events_table = LoadFactOperator(
          task_id = 'load_events_fact_table',
          redshift_conn_id = config.get("REDSHIFT", "CONNECTION"),
          table = 'events',
          insert_sql = SqlQueries.event_table_insert,
          dag = dag
          )

drop_stage_tables = PostgresOperator(
          task_id = "drop_stage_tables",
          dag = dag,
          postgres_conn_id = config.get("REDSHIFT", "CONNECTION"),
          sql = "drop_stage_tables.sql"
          )

end_operator = DummyOperator(
          task_id = 'stop_execution', 
          dag = dag
          )

start_operator >> download_git_data >> spark_process 
spark_process >> del_crc_task >> upload_to_S3_task
spark_process >> del_suc_task >> upload_to_S3_task
upload_to_S3_task >> create_main_tables >> create_stage_tables
create_stage_tables >> stage_events_dict_to_redshift >> stage_events_to_redshift
stage_events_to_redshift >> load_actor_dimension_table >> load_events_table
stage_events_to_redshift >> load_repo_dimension_table >> load_events_table
stage_events_to_redshift >> load_dict_dimension_table >> load_events_table
load_events_table >> drop_stage_tables >> end_operator 
load_events_table >> del_json_task >> end_operator 