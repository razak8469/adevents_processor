package adevents

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spray.json._
import AdEvents._


object AdEventsProcessor {
  def main(args: Array[String]): Unit = {
    val cassandraHost = "127.0.0.1"

    val conf = new SparkConf().setAppName("adevents_processor")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cassandraHost)
      .set("spark.eventLog.enabled", "true")
     // .set("spark.streaming.concurrentJobs", "4")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(10))

    val conn = CassandraConnector(conf).withSessionDo{ sess =>
      sess.execute("DROP KEYSPACE IF EXISTS adevents")
       sess.execute(
         """ CREATE KEYSPACE adevents WITH REPLICATION = {'class': 'SimpleStrategy',
           |'replication_factor': 1}
         """.stripMargin
       )
      sess.execute("DROP TABLE IF EXISTS adevents.adserves")
      sess.execute(
        """CREATE TABLE IF NOT EXISTS adevents.adserves (
          |time_stamp TIMESTAMP,
          |sessionid TEXT,
          |adid TEXT,
          |publisherid TEXT,
          |PRIMARY KEY(sessionid, adid, publisherid))""".stripMargin)
      sess.execute(
        """CREATE TABLE IF NOT EXISTS adevents.urlopens (
          |time_stamp  TIMESTAMP,
          |sessionid TEXT,
          |url TEXT, PRIMARY KEY(sessionid))""".stripMargin)
      sess.execute(
        """CREATE TABLE IF NOT EXISTS adevents.videoviews (
          |time_stamp TIMESTAMP,
          |sessionid TEXT,
          |videoid TEXT,
          |viewfrom FLOAT,
          |viewto FLOAT, PRIMARY KEY(sessionid, videoid))""".stripMargin)
    }

    val adserveTopic = "AD_TOPIC"
    val urlopenTopic = "URL_TOPIC"
    val videoViewTopic = "VIDEO_TOPIC"

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092"
   //   ,"auto.offset.reset" -> "smallest"
    )

    val adServeStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, Set(adserveTopic))

    val adServeColumns = SomeColumns("time_stamp" as "timestamp",
      "sessionid" as "sessionId", "adid" as "adId", "publisherid" as "publisherId")

    adServeStream.map{ case(_, v) =>
      v.parseJson.convertTo[AdServe]
    }.saveToCassandra("adevents", "adserves", adServeColumns)

    val urlOpenStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(urlopenTopic))

    val urlOpenColumns = SomeColumns("time_stamp" as "timestamp",
      "sessionid" as "sessionId", "url" as "url")

    urlOpenStream.map{ case(_, v) =>
      v.parseJson.convertTo[UrlOpen]
    }.saveToCassandra("adevents", "urlopens", urlOpenColumns)

    // process video view events
    val videoViewStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(videoViewTopic))

    val videoViewColumns = SomeColumns("time_stamp" as "timestamp",
      "sessionid" as "sessionId", "videoid" as "videoId", "viewfrom" as "from", "viewto" as "to")

    videoViewStream.map{ case(_, v) =>
      v.parseJson.convertTo[VideoView]
    }.saveToCassandra("adevents", "videoviews", videoViewColumns)

    adServeStream.print()
    ssc.start()
    ssc.awaitTermination(60*5*1000)
    ssc.stop()
  }
}
