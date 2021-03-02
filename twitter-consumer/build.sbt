name := "Kafka-Producer"

version := "1"

scalaVersion := "2.12.12"

 libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.0.1",
    "org.apache.spark" %% "spark-streaming" % "3.0.1",
    "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0",
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1"
)
