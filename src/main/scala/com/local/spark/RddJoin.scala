package com.local.spark
import org.apache.spark.{SparkConf, SparkContext}
object RddJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkRDDJoinOps").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //建立一个基本的键值对RDD，包含ID和名称，其中ID为1、2、3、4
    val rdd1 = sc.makeRDD(Array(("1","Spark"),("2","Hadoop"),("2","hive"),("3","Scala"),("4","Java")),2)

    //建立一个行业薪水的键值对RDD，包含ID和薪水，其中ID为1、2、3、5
    val rdd2 = sc.makeRDD(Array(("1","30K"),("2","15K"),("3","25K"),("5","10K")),2)

    println("//下面做Join操作，预期要得到（1,×）、（2,×）、（3,×）")

    val joinRDD=rdd1.join(rdd2).collect.foreach(println)

    println("//下面做leftOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(4,×）")

    val leftJoinRDD=rdd1.leftOuterJoin(rdd2).collect.foreach(println)

    println("//下面做rightOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(5,×）")

    val rightJoinRDD=rdd1.rightOuterJoin(rdd2).collect.foreach(println)


    sc.stop()
  }

}
