import pyspark
import pyspark.sql.functions as f
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import datetime
import os
import sys
import logging

def main():
	logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
	logging.info('Start of the program')

	try:
		year = sys.argv[1]
		month = sys.argv[2] 
		day = sys.argv[3] 
	except:
		logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM, DD)')

	if (int(year) >=2010) & (int(year)<= datetime.datetime.now().year) \
		& (int(month)<=12) & (int(month)>=1) & (len(month)==2) \
		& (int(day)<=31) & (int(day)>=1) & (len(day)==2):

		path = "{}-{}-{}".format(year, month, str(day).zfill(2))

		if os.path.isdir(path):
			conf = pyspark.SparkConf().setAppName('Git').setMaster('local')
			sc = pyspark.SparkContext(conf = conf)
			spark = SparkSession(sc)
			logging.info('******* Spark session created ******* ')

			df = spark.read.json(path)
			df = df.filter(((df.type=="PullRequestEvent") & (df.payload.action=='opened')) | \
						((df.type=="IssuesEvent") & (df.payload.action=='opened')) | \
						(df.type=="ForkEvent")) \
					.selectExpr(["created_at", "actor['id'] as actor_id", "actor['login'] as actor_login", \
							"actor['display_login'] as display_login","repo['id'] as repo_id", \
							"repo['name'] as repo_name", "type"])

			output_file = "df_{}.parquet".format(path)
			df.write.parquet(output_file)
			logging.info('******* Github data processed successfully. *******')
		else:
			logging.info('Directory not exists')
	else:
		logging.info('Wrong date')

		logging.info('End of program')

if __name__ == "__main__":
		main()