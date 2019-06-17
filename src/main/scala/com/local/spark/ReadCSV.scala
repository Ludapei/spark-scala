package com.local.spark

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ReadCSV {
  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setAppName("SQL").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //    val filePath="C:\\MyApplication\\Idea\\Project_bigdata\\sparkdemo3\\data.txt"

//    val rdd = sqlContext.read.format("com.databricks.spark.csv").option("sep", ",")
//      .option("header", "false")
//      .load("C:\\MyApplication\\Idea\\Project_bigdata\\sparkdemo3\\data3.csv")


//    val linRDD=sc.textFile(filePath).map(_.split(" "))

    val filePath="C:\\MyApplication\\Idea\\Project_bigdata\\sparkdemo3\\data3.csv"
    val csv = sc.textFile(filePath) // original file
    val data = csv.map(line => line.split(",").map(elem=>elem.trim))

    val personRDD=data.map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    import sqlContext.implicits._

    val personDF=personRDD.toDF()

    personDF.registerTempTable("t_person")

    val df=sqlContext.sql("select * from t_person order by id  limit 2")

    df.show()
    df.printSchema()

    val prop = new Properties()

    prop.put("user", "root")

    prop.put("password", "123456")
    //    df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/seckill","person",prop)

    sc.stop()


  }


  case class Person(id:Int,name:String,age:Int)
}

