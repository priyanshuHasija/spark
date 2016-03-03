package com.infoobjects.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Priyanshu
 */
object dataFrames {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local[2]").setAppName("test")
  val sc = new SparkContext(conf) // An existing SparkContext.
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.jsonFile("/home/Priyanshu/Desktop/sample.json")

// Displays the content of the DataFrame to stdout
df.select("alrmDesc").show()

val filterResults = df.filter(df("alrmTypId") < 2)

filterResults.show();

  }
 
}