package com.infoobjects.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * @author Priyanshu
 */
case class alarm(alrmDesc:String,alrmDetectionScDate:String)

object SqlContextTest {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("sparkSql")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
   import sqlContext.implicits._ 
  
    
    val readTextFile = sc.textFile("/home/Priyanshu/Desktop/sample.txt")
    val a = readTextFile.map { x => x.split(",") }
    val b = a.map{ x => alarm(x(0).trim(),x(1).trim()) }
    
    
    val df = b.toDF()
    
    df.registerTempTable("Alarm_Table")
    
//    sqlContext.sql("Select * from Alarm_Table").show()
    
    df.filter(df("alrmDesc").like("%S68%")).show()
//    val results = df.collect()
//    
//    results.take(1)
    
  }
  
}