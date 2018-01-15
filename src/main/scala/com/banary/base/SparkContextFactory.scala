package com.banary.base

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextFactory {

    def getSparkContext(appName: String): SparkContext = {
        val sparkConf = new SparkConf().setMaster("local").setAppName(appName);
        return new SparkContext(sparkConf);
    }

}
