package com.banary.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

public class UrlMain {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static final String HDFS_URI = "com.banary.hdfs://106.14.177.112:9000";

    public static void main(String[] args) {

        try(InputStream in = new URL(HDFS_URI + "/root/words.txt").openStream()){
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
