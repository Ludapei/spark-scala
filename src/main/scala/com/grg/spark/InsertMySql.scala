package com.grg.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.sql.Connection
import java.sql.DriverManager

object InsertMySql {
  def main(args: Array[String]): Unit = {
//    var conf = new SparkConf().setAppName("Hello World")
//    var sc = new SparkContext(conf)
//    var input = sc.textFile("test/hello", 2)
//    var count = input.flatMap(name => name.split(" ")).map((_, 1)).reduceByKey(((a, b) => a + b))
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val input=sc.textFile("C:\\Users\\简从\\Desktop\\YT\\FileFromJava2.log")
    val count= input.flatMap(name => name.split(" ")).map((_, 1)).reduceByKey((a,b)=>a+ b)
    count.foreachPartition(insertToMysql)


  }


  def insertToMysql(iterator: Iterator[(String, Int)]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://127.0.0.1:3306/ldp?useUnicode=true&characterEncoding=utf8"
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
        println("写入mysql成功............."+data._1+"  "+data._2)
      }
    })
  }
}






//CREATE TABLE `t_spark` (
//`id` bigint(20) NOT NULL AUTO_INCREMENT comment 'Product id' ,
//`name` varchar(120) NOT NULL comment 'Product name' ,
//`number` int(11) NOT NULL  comment 'product stock quantity',
//`create_time` timestamp NOT NULL  DEFAULT CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment 'create time',
//PRIMARY KEY (`id`)
//) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1 comment 'data from spark insert test';


