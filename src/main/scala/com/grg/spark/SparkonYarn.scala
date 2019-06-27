package com.grg.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkonYarn {
  def main(args: Array[String]): Unit = {
    val logFile = "hdfs://server202:8020/tmp/ldp/Data/data2"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN");
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("pudong")).count()
    val numbs = logData.filter(line => line.contains("lihua")).count()
    println("line with a: %s,line with b: %s".format(numAs, numbs))
  }
}
//client 可以看到完整日志
//    spark-submit \
//--class "com.grg.spark.SparkonYarn" \
//--master "yarn-client" \
///home/ldp/spark-demo3-1.0-SNAPSHOT.jar



//spark-submit \
//--class "com.grg.spark.SparkonYarn" \
//--master "yarn-cluster" \
///home/ldp/spark-demo3-1.0-SNAPSHOT.jar


//
//spark-submit \
//--class "com.grg.spark.SparkonYarn" \
//--master yarn-cluster \
//scala-1-1.0-SNAPSHOT.jar

