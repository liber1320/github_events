import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import datetime
import logging
import os
import sys 
import argparse

def parse_arguments():
	"""Function command line parameters."""

	parser = argparse.ArgumentParser()
	parser.add_argument('-y', '--year', help = 'year')
	parser.add_argument('-m', '--month', help = 'month')
	parser.add_argument('-d', '--day', help = 'day')
	args = parser.parse_args()
	dict_args = vars(args)
	return dict_args

def date_validation(year, month, day):
	"""Function validates initial date parameters."""

	if (int(year) >= 2010) & \
		(int(year) <= datetime.datetime.now().year) & \
		(int(month) <= 12) & \
		(int(month) >= 1) & \
		(len(month) == 2) & \
		(int(day)<=31) & \
		(int(day)>=1) & \
		(len(day)==2):
		return 1
	else:
		return 0

def create_spark_session():
	"""Function creates spark objects for further spark operations."""

	conf = pyspark.SparkConf().setMaster('local')
	sc = pyspark.SparkContext(conf = conf)
	spark = SparkSession(sc)
	logging.info('******* Spark session created ******* ')
	return spark

def process_data(df):
	"""Function filters data choicing, selects set of columns for further transformations and adds aliases"""

	df_f = df.filter(((df.type=="PullRequestEvent") & (df.payload.action=='opened')) | \
					((df.type=="IssuesEvent") & (df.payload.action=='opened')) | \
					(df.type=="ForkEvent")) \

	df_s = df_f.selectExpr(["created_at", \
						"actor['id'] as actor_id",  \
						"actor['login'] as actor_login", \
						"repo['id'] as repo_id", \
						"repo['name'] as repo_name", \
						"type"])

	df_a = df_s.withColumn("actor_id", df_s.actor_id.cast("string")) \
				.withColumn("repo_id", df_s.repo_id.cast("string"))

	return df_a

def process_json(year, month, day, spark):
	"""Function processes json file for each day: 
	- filtering data, 
	- calculating field, 
	- selecting columns 
	- writing results to json file."""

	path = "{}-{}-{}".format(year, month, str(day).zfill(2))
	try:
		df = spark.read.json(path)
	except Exception as exc:
		logging.info('No data for path %s Exception: %s' % (path, exc))
	df_p = process_data(df)

	output_file = "df_{}".format(path)
	df_p.write.json(output_file)
	logging.info('******* Github data processed successfully. *******')

def main():
	""" Run pipeline for given month and year in format 'YYYY', 'MM' """
	logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
	logging.info('Start of program')

	args = parse_arguments()
	year = args['year']
	month = args['month']
	day = args['day']

	if (month != None) & (year != None) & (day != None):
		if date_validation(year, month, day) == 1:
			spark = create_spark_session()
			process_json(year, month, day, spark)
		else:
			logging.info('Wrong date. Check if parameters are passed correctly')
	else:
		logging.info('No parameters found. ')

	logging.info('End of program')

if __name__ == "__main__":
	main()