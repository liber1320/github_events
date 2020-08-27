name := "spark_scala"

scalaVersion := "2.12.10"
version := "1.0"
libraryDependencies ++= Seq( "org.apache.spark" % "spark-core_2.12" % "3.0.0",
                             "org.apache.spark" % "spark-sql_2.12" % "3.0.0",
                             "org.wvlet.airframe" %% "airframe-log" % "19.7.3"
                            )
