import os
import sys 
import urllib.request
import datetime
import logging
from calendar import monthrange

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

	try:
		year = sys.argv[1]
		month = sys.argv[2]
	except:
		logging.info('Wrong parameters. Check if date is passed as year and month (YYYY, MM) and file name is passed.')
		
	if (int(year) >=2010) & (int(year)<= datetime.datetime.now().year) & (int(month)<=12) & (int(month)>=1) & (len(month)==2):
		download_data(year, month)
	else:
		logging.info('Wrong date. Check if date is passed as year and month (YYYY, MM)')

	logging.info('End of program')

if __name__ == "__main__":
	main()