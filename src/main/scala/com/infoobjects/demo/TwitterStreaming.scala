package com.infoobjects.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter._

/**
 * @author Priyanshu
 */
object TwitterStreaming {
  def main(args: Array[String]): Unit = {
    
    val sc = new SparkConf().setAppName("Twitter Streaming")
    val ssc = new StreamingContext(sc,Seconds(1))
    
    TwitterUtils.createStream(ssc,None)
    
    
    
    
  }
}