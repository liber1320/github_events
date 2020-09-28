# Github NoSql database using MongoDB - schema and ETL

**Overview**

This approach provides the schema and ETL to populate an analytics NoSql database in cloud for the github events data.
It has been designed as a Mongo database, which allows to analyze user's activities on github. 
The scripts have been created in Python. PySpark has been used in order to effectively process data before insert to database.

**Structure**

* [github_daily_download](github_daily_download.py) - downloads data for year month and day passed as parameter;
* [github_daily_process](github_daily_process_json.py) - process data for year month and day passed as parameter with use of PySpark;
* [config file](mongo.cfg) - contains paramaters for cluster and input file;
* [github_etl](github_daily_etl_mongo.py) - inserts data into the MongoDB cluster;
* [analytic queries](mongo.ipynb) - examples of NoSql queries.

**Schema**

The database contains the following document schema:
* `created_at` 
* `actor_id`
* `actor_login`
* `repo_id`
* `repo_name`
* `type`
         
**Instructions**

To run the project please do the following order:
1. Run github_daily_download.py (script requires year month and day as argument)
2. Run github_daily_process_json.py (script requires year month and day as argument)
3. Fill in mongo.cfg file with Mongo cluster parameters and path to directory with input file
4. Run github_daily_etl_mongo.py (script uses parameters from mongo.cfg file)

