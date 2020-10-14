import os
import sys 
import urllib.request
import datetime
import logging

def download_data(year, month, day):
	"""Function downloads data from http://data.gharchive.org for defined month, year, day.
	New directories for each day is created in current working directory and gzip data is downloaded there."""

	opener = urllib.request.URLopener()
	opener.addheader('User-Agent', 'whatever')

	cur_dir = os.getcwd()
	date = '{}-{}-{}'.format(year, month, str(day).zfill(2))
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
			logging.info('There was a problem for day %s hour %s: %s ' % (day, i, exc))
	logging.info('******* Data downloading finished. *******')

def main():
	""" Run pipeline for given month  year and day in format 'YYYY', 'MM', 'DD' """
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
		download_data(year, month, day)
	else:
		logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM, DD)')

	logging.info('End of program')

if __name__ == "__main__":
	main()