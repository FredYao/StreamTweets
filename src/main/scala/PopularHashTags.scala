package main.scala

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._

object PopularHashTags {

  /**
    * Set credentials for fetching Twitter's streaming data.
    */
  def setCredentials(): Unit = {
    val consumerKey = "Enter your consumer key here"
    val consumerSecret = "Enter your consumer secret here"
    val accessToken = "Enter your access token here"
    val accessTokenSecret = "Enter your access token secret here"

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  }

  def main(args: Array[String]) {
    // settings
    setCredentials()
    val duration = 10 // time interval (in seconds) for collecting streaming data
    val top = 5       // the number of popular hashtags to print

    // Spark configuration and streaming context
    val conf = new SparkConf().setMaster("local[*]").setAppName("Popular Hash Tags in Twitter")
    val ssc = new StreamingContext(conf, Seconds(duration))

    // Create twitter data stream with nothing filtered
    val stream = TwitterUtils.createStream(ssc, None, Seq())

    // Scan each tweet and find hashtags (i.e., starting with '#')
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Count #tags
    val tagCounts = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(duration))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    // Find popular hashtags
    tagCounts.foreachRDD(rdd => {
      val topList = rdd.take(top)
      println("\nPopular hashtags in last %s seconds (%s total):".format(duration, rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }

}