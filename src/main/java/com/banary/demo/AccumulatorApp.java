package com.banary.demo;

import com.google.common.base.Strings;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * 累加器, 累加空格
 *
 * Created by xiyongchun on 2017/8/29.
 */
public class AccumulatorApp {

    public static void main(String[] args) {
        String fileUrl = "words.txt";
        new AccumulatorApp().count(fileUrl);
    }

    public void count(String fileUrl){
        JavaSparkContext sc = AppUtils.initJavaSparkContext();
        JavaRDD<String> rdd = sc.textFile(fileUrl);

        final Accumulator blankLinesCount = sc.accumulator(0);
//        JavaRDD<String> callSigns = rdd.flatMap(line->{
//            if(Strings.isNullOrEmpty(line)){
//                blankLinesCount.add(1);
//            }
//            return Arrays.asList(line.split(",")).iterator();
//        });
//        callSigns.saveAsTextFile("output.txt");
        System.out.println(blankLinesCount.value());
    }

}
