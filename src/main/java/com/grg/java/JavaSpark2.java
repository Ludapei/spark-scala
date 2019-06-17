package com.grg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSpark2 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TxtToParquet").setMaster("local");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
//        reflectTransform(spark);//Java反射
//        dynamicTransform(spark);//动态转换

        JavaRDD<String> source = spark.read().textFile("C:\\\\MyApplication\\\\Idea\\\\Project_bigdata\\\\sparkdemo3\\\\data3.csv").javaRDD();

        JavaRDD<Student> rowRDD = source.map(line -> {
            String parts[] = line.split(",");
            Student stu = new Student();
            stu.setSid(parts[0]);
            stu.setSname(parts[1]);
            stu.setSage(Integer.valueOf(parts[2]));
            return stu;
        });

        Dataset<Row> df = spark.createDataFrame(rowRDD, Student.class);

        df.select("sid", "sname", "sage");
        df.show();

    }
}
