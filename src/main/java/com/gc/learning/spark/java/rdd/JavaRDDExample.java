package com.gc.learning.spark.java.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.util.Arrays;
import java.util.List;

/**
 * Created by ganeshchand on 10/31/15.
 */
public class JavaRDDExample {
    public static void main(String[] args){
        System.out.println("Hello World");

        SparkConf conf = new SparkConf().setAppName("Java RDD").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

       JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4));
        System.out.println("Count: "+rdd.count());
        List<Integer> list = rdd.collect();
        System.out.println(list.size());

        for(Integer item: list){
            System.out.println(item);
        }

    }
}
