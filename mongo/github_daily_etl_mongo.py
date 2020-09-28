from pymongo import MongoClient, InsertOne, UpdateOne
from os import listdir
from os.path import isfile, join
from bson.json_util import loads
import configparser
import logging

def insert_data(github_coll, path):

	batch_size = 10000
	inserts = []
	count = 0
	batch = 0
	files = [ path +'/'+ f for f in listdir(path) if isfile(join(path, f))]

	for file in files:
		with open(file) as dataset: 
			for line in dataset: 
				inserts.append(InsertOne(loads(line)))
				count += 1
				batch += 1
				if count == batch_size:
					github_coll.bulk_write(inserts)
					inserts = []
					count = 0
					logging.info('{} documents inserted'.format(batch))
		if inserts:
			github_coll.bulk_write(inserts)
			count = 0
			inserts = []
			logging.info('{} documents inserted'.format(batch))
	logging.info('Data inserted')
	
def main():
	logging.basicConfig(level=logging.INFO, format='%(asctime)s -  %(levelname)s-%(message)s')
	logging.info('Start of program')

	config = configparser.ConfigParser()
	config.read('mongo.cfg')
	
	client = MongoClient('mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority'.format(*config['CLUSTER'].values()))
	
	db = '{}'.format(*config['DB'].values())
	coll = '{}'.format(*config['COLL'].values())
	github_coll = client[db][coll]
	
	logging.info('Connections established')
	
	path =  '{}'.format(*config['PATH'].values())
	insert_data(github_coll, path)

	logging.info('End of program')

if __name__ == "__main__":
	main()