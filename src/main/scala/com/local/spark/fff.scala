package com.local.spark

package com.local

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.cmd.Property

object fff {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SQL").setMaster("local[4]")

    val sc=new SparkContext(conf)

    val sqlContext=new SQLContext(sc)

    val filePath1="C:\\MyApplication\\Idea\\project2\\sparkdemo3\\data1.txt"

    val filePath2="C:\\MyApplication\\Idea\\project2\\sparkdemo3\\data2.txt"

    val lineRDD1=sc.textFile(filePath1).map(_.split(" "))

    val lineRDD2=sc.textFile(filePath2).map(_.split(" "))

    val personRDD1=lineRDD1.map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    val personRDD2=lineRDD2.map(x=>Salary(x(0).toInt,x(1).toInt))

    import sqlContext.implicits._
    val personDF1=personRDD1.toDF
    personDF1.foreach(x=>println(x))

    val personDF2=personRDD2.toDF

    personDF2.foreach(x=>println(x))
    personDF1.registerTempTable("person")

    personDF2.registerTempTable("salary")
//    val df=sqlContext.sql("select * from salary s join person p on s.id=p.id;")
val df=sqlContext.sql("select * from salary s , person p")
    df.foreach(x=>println(x))


//    val prop=new Properties()
//
//    prop.put("user","root")
//    prop.put("password","123456")
//
//    df.write.mode("append").jdbc("jdbc:mysql://localhost:3306/seckill","person",prop)

  }


  case class Person(id:Int,name:String,age:Int)
  case class Salary(id:Int,money:Int)

}

