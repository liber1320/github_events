import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object Aggregations {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val spark = SparkSession
                .builder
                .master("local[*]")
                .appName("Github data processing")
                .getOrCreate()
    val sc = spark.sparkContext

    val df_filter = spark.read.parquet("files/df_filter_.parquet")

    val ds: Dataset[Git_Schema] = df_filter.as[Git_Schema](Encoders.product[Git_Schema])

    val agg = ds.groupByKey(x => (x.actor_id, x.actor_login, x.daily_date, x.repo_id, x.repo_name))(Encoders.product[(String, String, String, String, String)])
                .agg(count( "pull_request"),
                     count( "issue_request"),
                     count( "fork_request"))

    val newNames = Seq("actor_id", "actor_login", "daily_date", "repo_id", "repo_name",
                       "sum_pull_request", "sum_issues", "sum_fork")

    val agg_df = agg.map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._2, x._3, x._4))(Encoders.product[(String, String, String, String, String, Long, Long, Long)])
                    .toDF(newNames: _*)

    val agg_df_ = agg_df.withColumn("dist_fork", when(agg_df("sum_fork")>=1 ,1)
                        .otherwise(0))

    Logger.getRootLogger.info("*********************** stage aggregation done **********************")

    val dsa: Dataset[Git_agg_Schema] = agg_df_.as[Git_agg_Schema](Encoders.product[Git_agg_Schema])

    val agg_user = dsa.groupByKey(x => (x.actor_id, x.actor_login, x.daily_date))(Encoders.product[(String, String, String)])
                      .agg(sum("sum_pull_request").alias("sum_pull_request").as(Encoders.LONG),
                           sum("sum_issues").alias("sum_issues").as(Encoders.LONG))

    agg_user.selectExpr("key['_1'] as actor_id" , "key['_2'] as actor_login", "key['_3'] as daily_date",
                        "sum_pull_request", "sum_issues")
            .write
            .partitionBy("daily_date")
            .mode("overwrite")
            .parquet("files/user_agg.parquet")

    Logger.getRootLogger.info("*********************** user aggregation done **********************")

    val agg_repo = dsa.groupByKey(x => (x.repo_id, x.repo_name, x.daily_date))(Encoders.product[(String, String, String)])
                      .agg(sum("sum_pull_request").alias("sum_pull_request").as(Encoders.LONG),
                           sum("sum_issues").alias("sum_issues").as(Encoders.LONG),
                           sum("dist_fork").alias("sum_fork").as(Encoders.LONG))

    agg_repo.selectExpr("key['_1'] as repo_id" , "key['_2'] as repo_name", "key['_3'] as daily_date",
                        "sum_pull_request", "sum_issues", "sum_fork")
            .write
            .partitionBy("daily_date")
            .mode("overwrite")
            .parquet("files/repo_agg.parquet")

    Logger.getRootLogger.info("*********************** repo aggregation done **********************")
  }
}
