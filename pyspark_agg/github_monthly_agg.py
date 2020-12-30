import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from calendar import monthrange
import argparse
import datetime
import logging
import os
import sys 

def parse_arguments():
	"""Function parses command line arguments."""

	parser = argparse.ArgumentParser()
	parser.add_argument('-y', '--year', help = 'year')
	parser.add_argument('-m', '--month', help = 'month')
	parser.add_argument('-f', '--file', help = 'output file name', default='git_agg')
	args = parser.parse_args()
	dict_args = vars(args)
	return dict_args

def date_validation(year, month):
	"""Function validates initial date parameters."""

	if (int(year) >= 2010) & \
		(int(year) <= datetime.datetime.now().year) & \
		(int(month) <= 12) & \
		(int(month) >= 1) & \
		(len(month) == 2):
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
			.withColumn("daily_date", df.created_at.substr(1 ,10))

	df_s = df_f.selectExpr(["daily_date", \
						"actor['id'] as actor_id",  \
						"actor['login'] as actor_login", \
						"repo['id'] as repo_id", \
						"repo['name'] as repo_name", \
						"type"]) 
	return df_s

def process_json(output_file, year, month, spark):
	"""Function processes json file for each day: 
	- filtering data, 
	- calculating field, 
	- selecting columns 
	- writing results to parquet file."""

	days = monthrange(int(year),int(month))[1]

	for i in range(1, days+1):
		path = "{}-{}-{}".format(year, month, str(i).zfill(2))
		try:
			df = spark.read.json(path)
		except Exception as exc:
			logging.info('No data for path %s Exception: %s' % (path, exc))
		df_p = process_data(df)
		df_p.write.parquet(output_file, mode='append', partitionBy=["daily_date"])

		logging.info('******* Day %s processed. *******' % (path))

	logging.info('******* Github data processed. *******')

def basic_agg(filename_in, filename_agg, spark):
	"""Function reads parquet file and creates basic aggregation file for further analysis."""

	df_full = spark.read.parquet(filename_in)
	df_agg = df_full.groupBy(["daily_date", "actor_id", "actor_login", "repo_id", "repo_name"]) \
					.pivot("type", values= ["ForkEvent", "PullRequestEvent", "IssuesEvent"]) \
					.agg(f.count("actor_id")) 
	df_agg.write.parquet(filename_agg, partitionBy=["daily_date"])

	logging.info('******* Basic aggregation done. *******')

def create_agg_view(file, spark):
	"""Function reads parquet file and creates basic aggregation spark view for further analysis."""

	df_basic = spark.read.parquet(file) \
					.createOrReplaceTempView("basic_agg")
	logging.info('******* Basic aggregation view created. *******')

def user_agg(output_file, spark):
	"""User aggregation calculates with use of inital aggregation view.
	Results are written into parquet file."""

	spark.sql("""SELECT daily_date, actor_id, actor_login,
                 NVL(sum(PullRequestEvent),0) as PullRequestEvent_sum,
                 NVL(sum(IssuesEvent),0) as IssuesEvent_sum
                FROM basic_agg
                WHERE PullRequestEvent IS NOT NULL OR
                      IssuesEvent IS NOT NULL 
                GROUP BY daily_date, actor_id, actor_login
                ORDER BY daily_date, sum(PullRequestEvent) DESC""") \
		.write.parquet(output_file, mode='overwrite', partitionBy=["daily_date"])

	logging.info('******* User aggregation done. *******')

def repo_agg(output_file, spark):
	"""Repository aggregation calculates with use of inital aggregation view.
	Results are written into parquet file."""

	spark.sql("""SELECT daily_date, repo_id, repo_name,
                 NVL(sum(PullRequestEvent),0) as PullRequestEvent_sum,
                 NVL(sum(IssuesEvent),0) as IssuesEvent_sum,
                 count(ForkEvent) as ForkEvent_count
                FROM basic_agg
                GROUP BY daily_date, repo_id, repo_name
                ORDER BY daily_date, sum(PullRequestEvent) DESC""") \
		.write.parquet(output_file, mode='overwrite', partitionBy=["daily_date"])

	logging.info('******* Repository aggregation done.*******')

def main():
	""" Run pipeline for given year, month in format 'YYYY', 'MM' """

	logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
	logging.info('Start of program')

	args = parse_arguments()
	year = args['year']
	month = args['month']
	file = args['file']

	if (month != None) & (year != None):
		file_name = file + "_" + year + "_" + month + ".parquet"
		file_name_agg = file + "_" + "basic_agg" + "_" + year + "_" + month + ".parquet"
		file_name_user = file + "_" + "user" + "_" + year + "_" + month + ".parquet"
		file_name_repo = file + "_" + "repo" + "_" + year + "_" + month + ".parquet"

		if date_validation(year, month) == 1:
			spark = create_spark_session()
			process_json(file_name, year, month, spark)
			basic_agg(file_name, file_name_agg, spark)
			create_agg_view(file_name_agg, spark)
			user_agg(file_name_user, spark)
			repo_agg(file_name_repo, spark)
		else:
			logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM)')
	else:
		logging.info('No parameters found. ')

	logging.info('End of program')

if __name__ == "__main__":
	main()