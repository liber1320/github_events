# Github cloud data warehouse - schema and ETL

**Overview**

This approach provides the schema and ETL to create and populate an analytics database in cloud for the github events data.
It has been designed as a AWS Redshift relational database in a star schema, which allows to analyze user's activities on github. 
The scripts have been created in Python. PySpark has been used in order to effectively process data before insert to S3.

**Structure**

* [github_sql_queries](github_sql_queries.py)  - defines the SQL queries used in ETL pipeline;
* [github_create_tables](github_create_dwh_tables.py) - creates tables in Redhsift cluster;
* [github_daily_download](github_daily_download.py) - downloads data for year month and day passed as parameter;
* [github_daily_process](github_daily_process.py) - process data for year month and day passed as parameter with use of PySpark;
* [events dict](data/events.csv) - dictionary for events data used in ETL;
* [github_etl](github_etl.py) - defines the ETL pipeline, inserts data into the Redshift;


**Schema**

The database contains the following fact table:
* `events` - user's events

Fact table `events` table has foreign keys to the following dimension tables:
* `actors`
* `repos`
* `events_dict`

In ETL process there have been also used additional staging tables in order to perform insert and update.

**Instructions**

To run the project please do the following order:
1. Run github_daily_download.py (script requires year month and day as argument)
2. Run github_daily_process.py (script requires year month and day as argument)
3. Put output parquet file and events.csv into S3
4. Fill in dwh.cfg file with AWS cluster parameters
5. Run github_create_dwh_tables.py
6. Run github_etl.py (script requires path to events dictionary and parquet file)

