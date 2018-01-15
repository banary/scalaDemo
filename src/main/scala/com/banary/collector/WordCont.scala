package com.banary.collector

import com.banary.base.SparkContextFactory
import org.apache.commons.logging.LogFactory

object WordCont {

    def main(args: Array[String]): Unit = {
        val log = LogFactory.getLog("WordCont");

        val sc = SparkContextFactory.getSparkContext("WordCont");
        val urlFile: String = "words.txt";
        val linesRDD = sc.textFile(urlFile);
        val count = linesRDD.flatMap(line=> line.split(" "))
            .map(word=>(word, 1))
            .reduceByKey(_+_).count();
        print(count);
    }

}
