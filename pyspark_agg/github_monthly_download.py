import os
import sys 
import urllib.request
import datetime
import logging
import argparse
from calendar import monthrange

def parse_arguments():
	"""Function command line parameters."""

	parser = argparse.ArgumentParser()
	parser.add_argument('-y', '--year', help = 'year')
	parser.add_argument('-m', '--month', help = 'month')
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

def download_data(year, month):
	"""Function downloads data from http://data.gharchive.org for defined month and year.
	New directories for each day is created in current working directory."""

	opener = urllib.request.URLopener()
	opener.addheader('User-Agent', 'whatever')

	cur_dir = os.getcwd()
	days = monthrange(int(year),int(month))[1]

	for d in range(1, days+1):
		date = '{}-{}-{}'.format(year, month, str(d).zfill(2))
		wd = '{}/{}'.format(cur_dir, date)
		if not os.path.isdir(wd):
			os.mkdir(wd)
		os.chdir(wd)

		for i in range(0, 24):
			url = 'http://data.gharchive.org/{}-{}.json.gz'.format(date, i)
			filename = '{}-{}.gz'.format(date, i)
			try:
				filename, headers = opener.retrieve(url, filename)
			except Exception as exc:
				logging.info('There was a problem for day %s hour %s: %s ' % (d, i, exc))
		logging.info('Data downloaded for day %s' % (d))
	logging.info('******* Data downloading finished. *******')

def main():
	logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
	logging.info('Start of program')

	args = parse_arguments()
	year = args['year']
	month = args['month']

	if (month != None) & (year != None):
		if date_validation(year, month) == 1:
			download_data(year, month)
		else:
			logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM)')
	else:
		logging.info('No parameters found. ')

	logging.info('End of program')

if __name__ == "__main__":
	main()