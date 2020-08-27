import java.time.YearMonth
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Events_processing {
  def main(args: Array[String]): Unit = {

    val year = 2018
    val month = 2
    val yearMonthObject = YearMonth.of(year, month)
    val daysInMonth = yearMonthObject.lengthOfMonth

    Logger.getRootLogger.setLevel(Level.INFO)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Github data processing")
      .getOrCreate()
    val sc = spark.sparkContext

    for (d <- 1 to daysInMonth)
    {
      val path_d = "./files/" + year.toString + "-" + "%02d".format(month) + "-" + "%02d".format(d)
      val df = spark.read.json(path_d)

      val df_filter = df.filter((df("type") === "PullRequestEvent" && df("payload.action") === "opened") ||
                        (df("type") === "IssuesEvent" && df("payload.action") === "opened") ||
                        (df("type") === "ForkEvent"))
                        .selectExpr("created_at", "actor['id'] as actor_id",
                          "actor['login'] as actor_login", "type", "repo['id'] as repo_id",
                          "repo['name'] as repo_name")

      val df_ext = df_filter.withColumn("daily_date", df("created_at").substr(1, 10))
                            .withColumn("pull_request", when(df("type")==="PullRequestEvent" ,1)
                              .otherwise(null))
                            .withColumn("issue_request", when(df("type")==="IssuesEvent" ,1)
                              .otherwise(null))
                            .withColumn("fork_request", when(df("type")==="ForkEvent" ,1)
                              .otherwise(null))
                            .withColumn("daily_date", df("created_at").substr(1, 10))

      df_ext.selectExpr("created_at", "actor_id",
            "actor_login", "repo_id", "pull_request", "issue_request",
            "fork_request","daily_date", "repo_name")
            .write
            .partitionBy("daily_date")
            .mode("append")
            .parquet("files/df_filter.parquet")

      Logger.getRootLogger.info("*********************** data processed for day " + d.toString +" **********************")
    }
    Logger.getRootLogger.info("*********************** data processed **********************")
  }
}
