# Github database - schema and ETL

**Overview**

This approach provides the schema and ETL to create and populate an analytics database for the github events data.
It has been designed as a PostgreSQL relational database in a star schema, which allows to analyze user's activities on github. 
The scripts have been created in Python, leveraging its convenient wrapper around Postgres. PySpark has been used in order to effectively process data before insert to database.

**Structure**

* [github_sql_queries](github_sql_queries.py)  - defines the SQL queries used in database creation and ETL pipeline;
* [github_create_tables](github_create_tables.py) - creates database and tables;
* [github_daily_download](github_daily_download.py) - downloads data for year month and day passed as parameter;
* [github_daily_process](github_daily_process.py) - process data for year month and day passed as parameter with use of PySpark;
* [events dict](data/events.csv) - dictionary for events data used in etl;
* [github_daily_etl](github_daily_etl.py) - defines the ETL pipeline, inserts data into the Postgres database;
* [analytic queries](data_analysis.ipynb) - examples of database queries.

**Schema**

The database contains the following fact table:
* `events` - user's events

Fact table `events` table has foreign keys to the following dimension tables:
* `actors`
* `repos`
* `events_dict`

**Instructions**

To run the project please run scripts in the following order:
1. github_create_tables.py
2. github_daily_download.py (script requires year month and day as argument)
3. github_daily_process.py (script requires year month and day as argument)
4. github_daily_etl.py (script requires path to parquet file created in previous step)

