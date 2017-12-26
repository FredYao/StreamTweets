package main.scala

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import twitter4j.FilterQuery

object TweetsLocation {

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

    // Geo-boundaries:
    val boundingBoxes = Array(Array(-74.0, 40.0), Array(-73.0, 41.0))

    // Create a query with the Geo-boundaries
    val locationsQuery = new FilterQuery().locations(boundingBoxes : _*)

    // Spark configuration and streaming context
    val conf = new SparkConf().setMaster("local[*]").setAppName("Tweets in Certain Location")
    val ssc = new StreamingContext(conf, Seconds(10))

    // Filter the tweets stream by the defined query
    TwitterUtils.createFilteredStream(ssc, None, Some(locationsQuery))
      .map(tweet => {
        val latitude = Option(tweet.getGeoLocation).map(l => s"${l.getLatitude},${l.getLongitude}")
        val place = Option(tweet.getPlace).map(_.getName)
        val location = latitude.getOrElse(place.getOrElse("(no location)"))
        val text = tweet.getText.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        s"$location\t$text"
      })
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
}