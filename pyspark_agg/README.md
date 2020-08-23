# Github events aggregation with PySpark

**Overview**

This approach provides pipeline to download and calculate aggregation with PySpark. Solution allows to download github events data for given month and year using Python.
Downloaded data are processed with PySpark to calculate aggregations for users and repositories. 
Results are written to parquet files.

**Structure**

* [github_monthly_download](github_monthly_download.py) - downloads data for year and month passed as parameter
* [github_monthly_agg](github_monthly_agg.py) - process data for year month passed as parameter with use of PySpark

**Description**

Pipeline concerns such events as:
* `ForkEvent`
* `PullRequestEvent`
* `IssuesEvent`

There are two aggregations, as a result of process:

1. `User` aggregation with parameters:
* daily_date
* actor_id
* actor_login
* sum of PullRequestEvent
* sum of IssuesEvent

2. `Repo` aggregation with parameters:
* daily_date
* repo_id
* repo_name
* sum of PullRequestEvent
* sum of IssuesEvent
* sum of ForkEvent

**Instructions**

To run the project please run scripts in the following order:
1. github_monthly_download.py (script requires year month  as argument)
2. github_monthly_agg.py (script requires year month and output file prefix as argument)
