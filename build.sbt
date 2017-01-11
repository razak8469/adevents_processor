name := "adevents"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= {
  val connectorVersion = "1.6.0-M1"
  val sparkVersion = "1.6.1"
  val akkaVersion = "2.4.12"
  Seq(
    "com.datastax.spark" % "spark-cassandra-connector_2.11" % connectorVersion,
    "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
    "org.apache.spark" % "spark-streaming-kafka_2.11" % sparkVersion,
    "org.apache.kafka" % "kafka_2.11" % "0.8.2.0",
    "io.spray" %%  "spray-json" % "1.3.3",
    "com.typesafe.akka" %% "akka-cluster" % akkaVersion
  )
}
