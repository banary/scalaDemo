package com.banary.demo;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 广播变量使用, 查询words.txt中某个单词的个数
 *
 * @author Eden
 * @date 2017/8/29 下午8:20
 */
public class BroadcastApp {

    public static void main(String[] args) {
        String fileUrl = "data/words.txt";
        System.out.println("文件：" + fileUrl + "的单词数目为：" + new BroadcastApp().count(fileUrl));
    }

    public long count(String fileUrl){
        JavaSparkContext sc = AppUtils.initJavaSparkContext();
        JavaRDD<String> rdd = sc.textFile(fileUrl);

        //1.创建广播变量
        final Broadcast<List<String>> broadcast = sc.broadcast(getToBroadcastValue());
        //如果这个单词在广播变量中记两次
        JavaPairRDD<String, Integer> result = rdd.flatMap(line->Arrays.asList(line.split(",")))
                .mapToPair(word->{ if(broadcast.getValue().contains(word)){
                return new Tuple2<>(word, 2);
            }
            return new Tuple2<>(word, 1);
        }).reduceByKey((Integer x, Integer y)->x+y);

        return result.count();
    }

    /**
     * 获取要广播的值
     *
     * @author Eden
     * @date 2017/8/29 下午8:22
     * @param
     * @return
     */
    public List<String> getToBroadcastValue(){
        return Arrays.asList("red", "black");
    }
}
