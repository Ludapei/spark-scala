package com.grg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 *   DataFrame/RDD/DataSet的异同
 *
 *   DataFrame~= Dataset<Row>
 *
 *   https://www.jianshu.com/p/b9a7a650a781
 */
public class JavaSpark2 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TxtToParquet").setMaster("local");
        SparkSession sc = SparkSession.builder().config(conf).getOrCreate();
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        //修改路径
        JavaRDD<String> source = sc.read().textFile("C:\\\\MyApplication\\\\Idea\\\\Project_bigdata\\\\sparkdemo3\\\\data3.csv").javaRDD();

        // 将数据与类名对应
        JavaRDD<Student> rowRDD = source.map(line -> {
            String parts[] = line.split(",");
            Student stu = new Student();
            stu.setSid(parts[0]);
            stu.setSname(parts[1]);
            stu.setSage(Integer.valueOf(parts[2]));
            return stu;
        });


        Dataset<Row> df = sc.createDataFrame(rowRDD, Student.class);

        df.select("sid", "sname", "sage");

        df.select("sname", "sage");
        df.show();


        System.out.println("use sparkSql");
        df.registerTempTable("t_person");  //赋予表名


        System.out.println("df2");
        Dataset<Row> df2=sqlContext.sql("select * from t_person order by sid  limit 2");
        df2.show();




        System.out.println("df3");
        df.createOrReplaceTempView("tmp");
        Dataset<Row> df3=sqlContext.sql("select sage,sname from tmp where sage>14");
        df3.show();


        Dataset<Row> df4 = sqlContext.sql("select sname,sum(sage) sumage from t_person group by sname");
        df4.show();

        Dataset<Row> df5 = sqlContext.sql("select *, 2 as grade from t_person ");
        df5.show();

        Dataset<Row> df6= sqlContext.sql("select * from t_person where sage<15");
        df6. createOrReplaceTempView("tmp1");

        Dataset<Row> df7= sqlContext.sql("select * from t_person where sage>14");
        df7. createOrReplaceTempView("tmp2");

        Dataset<Row> df8= sqlContext.sql("select * from tmp1 join tmp2  on tmp1.sid != tmp2.sid");
        df8.show();

        Dataset<Row> df9= sqlContext.sql("select * from tmp1 join tmp2  on tmp1.sid != tmp2.sid");
        df9.show();


    }
}
