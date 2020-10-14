import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import datetime
import logging
import os
import sys 

def create_spark_session():
	conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
	sc = pyspark.SparkContext(conf = conf)
	spark = SparkSession(sc)
	logging.info('******* Spark session created ******* ')
	return spark

def filter_data(df):
	"""Function filters data frame by choosing subset of events."""

	df = df.filter(((df.type=="PullRequestEvent") & (df.payload.action=='opened')) | \
		((df.type=="IssuesEvent") & (df.payload.action=='opened')) | \
		(df.type=="ForkEvent"))
	return df

def select_columns(df):
	"""Function select set of columns for further transformations and adds aliases"""

	df = df.selectExpr(["created_at", "actor['id'] as actor_id", "actor['login'] as actor_login", \
						"actor['display_login'] as display_login", "repo['id'] as repo_id", \
						"repo['name'] as repo_name", "type"]) 
	return df

def process_json(year, month, day, spark):
	"""Function processes json file for each day by: 
	- filtering data, 
	- calculating field,
	- selecting columns 
	- writing results to parquet file"""

	input_file = "{}-{}-{}".format(year, str(month).zfill(2), str(day).zfill(2))
	output_file = "df_{}.parquet".format(input_file)
	try:
		df = spark.read.json(input_file)
	except Exception as exc:
		logging.info('No data for path %s Exception: %s' % (input_file, exc))

	df = filter_data(df)
	df = select_columns(df)
	df.write.parquet(output_file)

	logging.info('******* Github data processed successfully. *******')

def main():
	""" Run pipeline for given month and year in format 'YYYY', 'MM' """
	logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
	logging.info('Start of program')

	try:
		year = sys.argv[1]
		month = sys.argv[2] 
		day = sys.argv[3] 
	except:
		logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM, DD)')
		
	if (int(year) >=2010) & (int(year)<= datetime.datetime.now().year) \
		& (int(month)<=12) & (int(month)>=1) & (len(month)==2) \
		& (int(day)<=31) & (int(day)>=1) & (len(day)==2):

		spark = create_spark_session()
		process_json(year, month, day, spark)
	else:
		logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM, DD)')
		
	logging.info('End of program')

if __name__ == "__main__":
	main()