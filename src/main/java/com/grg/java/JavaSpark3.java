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
public class JavaSpark3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TxtToParquet").setMaster("local");
        SparkSession sc = SparkSession.builder().config(conf).getOrCreate();
        SQLContext sqlContext = new SQLContext(sc);

        //修改路径
        JavaRDD<String> source = sc.read().textFile("C:\\MyApplication\\Idea\\Project_bigdata\\sparkdemo3\\data4.csv").javaRDD();

        // 将数据与类名对应
        JavaRDD<Data> rowRDD = source.map(line -> {
            String parts[] = line.split(",");
            Data data = new Data();
            data.setA(Integer.valueOf(parts[0]));
            data.setB(Integer.valueOf(parts[1]));
            data.setC(Integer.valueOf(parts[2]));
            return data;
        });


        Dataset<Row> df = sc.createDataFrame(rowRDD, Data.class);



        System.out.println("use sparkSql");
        df.registerTempTable("data");  //赋予表名


        System.out.println("df2");
        Dataset<Row> df2=sqlContext.sql("select a,b,c,a+b*2+c*3 d from data ");
        df2.show();





    }
}
