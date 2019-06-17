package com.local.spark

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

//data1
/*
1 wang 14
2 lu 15
3 lu 14
4 lu 16
5 wang 15
6 lu 14
7 wang 16
 */
//data2
/*
1 wang A
2 lu B
 */
object sqlUnionOperation {
  def main(args: Array[String]): Unit = {


    val conf=new SparkConf()
    val sc=new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val personArray=Array("1 wang 15","2 lu 15","3 lu 15","4 lu 16","5 wang 15","6 lu 14","7 wang 16")

    val gradeArray=Array("1 wang A","2 lu B")

    val personRDD=sc.makeRDD(personArray).map(_.split(" ")).map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    val gradeRDD=sc.makeRDD(gradeArray).map(_.split("")).map(x=>Grade(x(0).toInt,x(1),x(2).toInt))

    import sqlContext.implicits._

    personRDD.toDF()
    gradeRDD.toDF()


  }
  case class Grade(id:Int,name:String,age:Int)

  case class Person(id:Int,name:String,age:Int)
}
