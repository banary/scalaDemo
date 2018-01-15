package com.banary.mllib

import org.apache.spark.mllib.linalg.Vectors

class MLlibMain {

    def test():Void = {

        //创建稠密向量
        val vector1 = Vectors.dense(1.0, 2.0, 3.0);
        val vector2 = Vectors.dense(Array(1.0, 2.0, 3.0));

        //创建稀疏向量<1.0, 0.0, 2.0, 0.0>
        //参数，向量的维度，非零为的位置，对应的值
        val vector3 = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0));
    }

}
