package com.banary.demo;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 单词数统计
 *
 * @author Eden
 * @date 2017/8/20 上午1:03
 */
public class WordCountApp {

    public static void main(String[] args) {
        String fileUrl = "data/words.txt";
        System.out.println("文件：" + fileUrl + "的单词数目为：" +new WordCountApp().count(fileUrl));
    }

    public long count(String fileUrl){
        JavaSparkContext sc = AppUtils.initJavaSparkContext();

        JavaRDD<String> input = sc.textFile(fileUrl);
        //1.切分成单词
        JavaPairRDD<String, Integer> result = input.flatMap(line->Arrays.asList(line.split(",")))
                //2.转成键值对 key为单词，value为1，表示出现了一次
                .mapToPair(word->new Tuple2<>(word, 1))
                //3.统计次数
                .reduceByKey((Integer x, Integer y)->x+y);
        return result.count();
    }

    public void read(String DirUrl){
        JavaSparkContext sc = AppUtils.initJavaSparkContext();
        JavaPairRDD<String, String> javaPairRDD = sc.wholeTextFiles(DirUrl);
    }
}
