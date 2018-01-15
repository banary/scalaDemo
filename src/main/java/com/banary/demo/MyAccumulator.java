package com.banary.demo;

import org.apache.spark.AccumulatorParam;

/**
 * Created by xiyongchun on 2017/8/29.
 */
public class MyAccumulator implements AccumulatorParam{

    @Override
    public Object addAccumulator(Object t1, Object t2) {
        //return super.addAccumulator(t1, t2);
        return null;
    }

    @Override
    public Object addInPlace(Object r1, Object r2) {
        return null;
    }

    @Override
    public Object zero(Object initialValue) {
        return null;
    }
}
