import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from airflow.models import Variable
import datetime
import os
import sys
import logging

def main():

        logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
        logging.info('Start of program')

        year = Variable.get('year')
        month = Variable.get('month')
        day = Variable.get('day')

        conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
        sc = pyspark.SparkContext(conf = conf)
        spark = SparkSession(sc)
        logging.info('******* Spark session created ******* ')

        path = "{}-{}-{}".format(year, month, str(day).zfill(2))
        try:
                df = spark.read.json(path)
        except Exception as exc:
                logging.info('No data for path %s Exception: %s' % (path, exc))

        df = spark.read.json(path)

        df = df.filter(((df.type=="PullRequestEvent") & (df.payload.action=='opened')) | \
                ((df.type=="IssuesEvent") & (df.payload.action=='opened')) | \
                (df.type=="ForkEvent"))

        df = df.selectExpr(["created_at", "actor['id'] as actor_id", "actor['login'] as actor_login", \
                                                "repo['id'] as repo_id", "repo['name'] as repo_name", "type"])

        output_file = "git_{}.parquet".format(path)
        df.write.parquet(output_file)

        logging.info('******* Github data processed successfully. *******')

        logging.info('End of program')

if __name__ == "__main__":
        main()
