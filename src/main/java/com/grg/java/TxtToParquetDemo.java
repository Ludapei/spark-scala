package com.grg.java;

import java.util.ArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
public class TxtToParquetDemo {
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



    /**
     * 通过Java反射转换
     * @param spark
     */
    private static void reflectTransform(SparkSession spark)
    {
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

        df.select("sid", "sname", "sage").collect();
        df.show();

    }
    /**
     * 动态转换
     * @param spark
     */
    private static void dynamicTransform(SparkSession spark)
    {
        JavaRDD<String> source = spark.read().textFile("C:\\\\MyApplication\\\\Idea\\\\Project_bigdata\\\\sparkdemo3\\\\data3.csv").javaRDD();

        JavaRDD<Row> rowRDD = source.map( line -> {
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
//        df.coalesce(1).write().mode(SaveMode.Append).parquet("parquet.res1");


    }

}
