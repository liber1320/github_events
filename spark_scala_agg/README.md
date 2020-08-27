# Github events aggregation with Spark and Scala

**Overview**

This approach provides pipeline to download and calculate aggregation with Scala and Scala Spark API. Solution allows to download github events data for given month and year using Scala.
Downloaded data are processed with Scala Spark API to calculate aggregations for users and repositories. 
Results are written to parquet files. Project has been configured in `IntelliJ IDEA` IDE.

**Structure**

* [Download_events](github_scala/src/main/scala/Download_events.scala) - downloads data for given year and month 
* [Events_processing](github_scala/src/main/scala/Events_processing.scala) - process data for year month with use of Scala Spark
* [Aggregations](github_scala/src/main/scala/Aggregations.scala) - creates aggreagtions

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
1. Download_events.scala (year and month are set at beginning of script)
2. Events_processing.scala (year and month are set at beginning of script)
3. Aggregations.scala
