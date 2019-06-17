package com.grg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;

public class JavaSpark {


    public static void main(String[] args) {

        JavaSpark javaSpark = new JavaSpark();

        SparkConf conf = new SparkConf().setAppName("TxtToParquet").setMaster("local");

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        javaSpark.reflectTransform(spark);//Java反射

        javaSpark.dynamicTransform(spark);//动态转换

    }


    /**
     * 通过Java反射转换
     *
     * @param spark
     */

    private void reflectTransform(SparkSession spark) {

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

    }

    /**
     * 动态转换
     *
     * @param spark
     */

    private static void dynamicTransform(SparkSession spark) {

        JavaRDD<String> source = spark.read().textFile("stuInfo.txt").javaRDD();


        JavaRDD<Row> rowRDD = source.map(line -> {

            String[] parts = line.split(",");

            String sid = parts[0];

            String sname = parts[1];

            int sage = Integer.parseInt(parts[2]);


            return RowFactory.create(

                    sid,

                    sname,

                    sage

            );

        });


        ArrayList<StructField> fields = new ArrayList<StructField>();

        StructField field = null;

        field = DataTypes.createStructField("sid", DataTypes.StringType, true);

        fields.add(field);

        field = DataTypes.createStructField("sname", DataTypes.StringType, true);

        fields.add(field);

        field = DataTypes.createStructField("sage", DataTypes.IntegerType, true);

        fields.add(field);


        StructType schema = DataTypes.createStructType(fields);


        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        df.coalesce(1).write().mode(SaveMode.Append).parquet("parquet.res1");


    }
    @SuppressWarnings("serial")
    class Student implements Serializable {


        String sid;

        String sname;

        int sage;

        public String getSid() {

            return sid;

        }

        public void setSid(String sid) {

            this.sid = sid;

        }

        public String getSname() {

            return sname;

        }

        public void setSname(String sname) {

            this.sname = sname;

        }

        public int getSage() {

            return sage;

        }

        public void setSage(int sage) {

            this.sage = sage;

        }

        @Override

        public String toString() {

            return "Student [sid=" + sid + ", sname=" + sname + ", sage=" + sage + "]";

        }

    }
}



