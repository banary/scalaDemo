package com.banary.demo;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.collection.Map;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

public class Test {

    static class Row{
        public String getString(int i){
            return "1231321";
        }
    }

    public static void main(String[] args) {
        System.out.println(getValue(new Test.Row(), 1, Long.class));
    }

    public static <T>T getValue(Row row, int column, Class<T> className){
        String value = row.getString(column);
        if(StringUtils.isEmpty(value)){
            return null;
        }

        if(className.isAssignableFrom(Double.class)){
            return (T)Double.valueOf(value);
        }

        if(className.isAssignableFrom(Long.class)){
            return (T)Long.valueOf(value);
        }
        return (T)value;
    }

}
