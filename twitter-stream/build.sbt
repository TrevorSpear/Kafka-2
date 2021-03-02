name := "Twitter-Stream"

version := "1"

scalaVersion := "2.12.12"

 libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
    "org.apache.httpcomponents" % "httpclient" % "4.5.12",
    "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
)
