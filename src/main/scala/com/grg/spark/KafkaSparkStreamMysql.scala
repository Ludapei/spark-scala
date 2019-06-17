package com.grg.spark

import java.sql.{Connection, DriverManager}

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaSparkStreamMysql {
  def main(args: Array[String]) {
    //    if (args.length < 2) {
    //      System.err.println(s"""
    //                            |Usage: DirectKafkaWordCount <brokers> <topics>
    //                            |  <brokers> is a list of one or more Kafka brokers
    //                            |  <topics> is a list of one or more kafka topics to consume from
    //                            |
    //        """.stripMargin)
    //      System.exit(1)
    //    }

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.1.40.201:3306/ldp_source?useUnicode=true&characterEncoding=utf8"
    val username = "root"
    val password = "123456"
    var connectionMqcrm: Connection = null
    Class.forName(driver)
    connectionMqcrm = DriverManager.getConnection(url, username, password)
    val sql = "INSERT INTO mes_kafka_spark (`message`) VALUES (?)"



    ///
    val Array(brokers, topics) = Array("server202:6667,server204:6667,server203:6667","topic-2")

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[4]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    //    if(lines.toString.contains()) {
    val statement = connectionMqcrm.prepareStatement(sql)
    statement.setString(1, lines.toString)
    var result = statement.executeUpdate()
//    if (result == 1) {
//      println("写入mysql成功    "+lines)
//
//    }


    //    }

    //    lines.foreachRDD(insertToMysql)
    //val words = lines.flatMap(_.split(" "))


    //val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

  def insertToMysql(iterator: Iterator[(String,Int)]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.1.40.201:3306/ldp_source?useUnicode=true&characterEncoding=utf8"
    val username = "root"
    val password = "123456"
    var connectionMqcrm: Connection = null
    Class.forName(driver)
    connectionMqcrm = DriverManager.getConnection(url, username, password)
    val sql = "INSERT INTO t_spark (`name`,`number`) VALUES (?,?)"
    iterator.foreach(data => {
      val statement = connectionMqcrm.prepareStatement(sql)
      statement.setString(1, data._1)
      statement.setInt(2, data._2)
      var result = statement.executeUpdate()
      if (result == 1) {
        println("写入mysql成功    "+data._1)
      }
    })
  }

}


//CREATE TABLE `mes_kafka_spark` (
//`id` bigint(20) NOT NULL AUTO_INCREMENT comment 'rownum' ,
//`message` varchar(120) NOT NULL comment 'Product name' ,
//`create_time` timestamp NOT NULL  DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'create time',
//PRIMARY KEY (`id`)
//) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 comment 'data from spark insert test';