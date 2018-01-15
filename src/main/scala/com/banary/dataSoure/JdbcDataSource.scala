package com.banary.dataSoure

import java.sql.{Connection, DriverManager, ResultSet}

import com.banary.base.SparkContextFactory
import org.apache.spark.rdd.JdbcRDD

/**
  * 1. 定义创建数据库链接函数，用于分发到各节点创建数据库拉去数据
  * 2. 定义解析数据函数，用于分发到各节点解析数据
  * 3. 定义创建JdbcRDD的函数，给驱动器程序调用
  */
class JdbcDataSource extends Serializable{

    def createConnection(): Connection = {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        return DriverManager.getConnection("jdbc:mysql://192.168.1.90:3306/db_nono?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&yearIsDateType=false&autoReconnect=true",
        "yanfa", "yanfa#123")
    }

    def extractValues(rs: ResultSet): (Long, Long) = {
        return (rs.getLong(1), rs.getLong(2))
    }

    def getJdbcRDD(): JdbcRDD[(Long, Long)] = {
        val sc = SparkContextFactory.getSparkContext("JdbcDataSource");
        return new JdbcRDD(sc, createConnection,
            "select id,user_id from borrows_accept where id>=? and id<=?",
            lowerBound = 55, upperBound = 154, numPartitions = 2, mapRow = extractValues);
    }
}

object Test{
    def main(args: Array[String]): Unit = {
        val rdd = new JdbcDataSource().getJdbcRDD();

        print(rdd.collect().toList.foreach(row=>{
            print("("+row._1 +","+ row._2+")\n");
        }))
    }


}
