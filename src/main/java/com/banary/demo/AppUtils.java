package com.banary.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by xiyongchun on 2017/8/29.
 */
public class AppUtils {

    public static JavaSparkContext initJavaSparkContext(){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCount");
        return new JavaSparkContext(sparkConf);
    }
}
