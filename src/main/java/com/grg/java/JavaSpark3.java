package com.grg.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Iterator;

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
        JavaRDD<String> source = sc.read().textFile("C:\\MyApplication\\Idea\\Project_bigdata\\sparkdemo3\\data5.csv").javaRDD();


        source.foreachPartition(new VoidFunction<Iterator<String>>() {
        @Override
        public void call(Iterator<String> iter) throws Exception {
            HashMap<String, String> hm = new HashMap<>();
            while(iter.hasNext()) {
                String[] next=iter.next().split(",");
                if (hm.get(next[0].trim()) != null) {
                    hm.put(next[0], hm.get(next[0]) + "," + next[1]);
                } else {
                    hm.put(next[0], next[1]);
                }
            }
            for (String key : hm.keySet()) {
                System.out.println(key + ":"+hm.get(key));
            }
         }
        });

//
//        Dataset<Row> df = sc.createDataFrame(rowRDD, Data.class);
//
//
//
//        System.out.println("use sparkSql");
//        df.registerTempTable("data");  //赋予表名
//
//
//        System.out.println("df2");
//        Dataset<Row> df2=sqlContext.sql("select a,b,c,a+b*2+c*3 d from data ");
//        df2.show();



    }
}
