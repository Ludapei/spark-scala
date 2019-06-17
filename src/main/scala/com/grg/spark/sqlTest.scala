package com.grg.spark
import java.sql.Connection

import com.grg.java.ConnectionPool
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

//
//https://www.cnblogs.com/hmy-blog/p/7798840.html

object sqlTest {
  def main(args: Array[String]){

    val conf = new SparkConf().setMaster("local[2]").setAppName("w")
    val ssc = new StreamingContext(conf,Seconds(5))
    var connectionMqcrm: Connection = null

    val lines = ssc.socketTextStream("10.1.40.202",9999)
   // lines.print()
    val words = lines.flatMap(_.split(" "))
    val wordcount = words.map(x => (x,1)).reduceByKey(_+_)
    wordcount.foreachRDD(rdd => {
      if(rdd.isEmpty()){
        print("rdd is empty")
      }else{
      rdd.foreachPartition(eachPartition => {
        val conn = ConnectionPool.getConnection()
        eachPartition.foreach(record => {
          print("record._1       "+record._1+" \n ")
          print("record._2       "+record._2+" \n ")


//          val sql="INSERT INTO t_spark (`name`,`number`) VALUES (?,?)"
//
//          val statement = connectionMqcrm.prepareStatement(sql)
//          statement.setString(1, record._1)
//          statement.setInt(2, record._2)

          val sql = "insert into t_spark (name,number) values('" + record._1 + "'," + record._2 + ")"
          val stmt = conn.createStatement
          val result=stmt.executeUpdate(sql)
          if (result == 1) {
            println("写入mysql成功............."+record._1+"  "+record._2)
          }
        })
        ConnectionPool.returnConnection(conn)
      })}
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

