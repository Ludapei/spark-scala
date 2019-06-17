package com.grg.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkonLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
    val text=sc.textFile("C:\\Users\\简从\\Desktop\\YT\\FileFromJava.log")
    val wordCount=text.count()
    println(wordCount)
  }


}
