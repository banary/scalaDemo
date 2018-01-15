package com.banary.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class FileSystemMain {


    public static void main(String[] args) throws Exception{
        useDistributedFileSystemByUri();
    }

    public static void useDistributedFileSystemByUri() throws Exception{
        String uri = "com.banary.hdfs://localhost:8020/root/words.txt";
        Configuration config = new Configuration();
        FSDataInputStream in = null;
        try{
            FileSystem fileSystem = FileSystem.get(URI.create(uri), config);
            in = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }

    /**
     * 通过静态方法获取本地文件系统对象
     * @throws Exception
     */
    public static void useLocalFileSystem() throws Exception{
        FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        FSDataInputStream in = null;
        try{
            in = fileSystem.open(new Path("/Users/xiyongchun/workspace/canary/scalaDemo/words.txt"));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }

    }

    /**
     * uri方案创建本地文件系统对象
     * @throws Exception
     */
    public static void useLocalFileSystemByUri() throws Exception{
        String uri = "file:/Users/xiyongchun/workspace/canary/scalaDemo/words.txt";
        Configuration config = new Configuration();
        FSDataInputStream in = null;
        FSDataOutputStream os = null;
        FSDataOutputStream os1 = null;
        FileSystem fileSystem = FileSystem.get(URI.create(uri), config);

        System.out.println();
        try{
            //读数据
            in = fileSystem.open(new Path(uri));
            //写数据
            os = fileSystem.create(new Path("file:/Users/xiyongchun/workspace/canary/scalaDemo/words1.txt"), ()->System.out.println("我创建成功了"));
            IOUtils.copyBytes(in, os, 4096, false);
            //移动到文件中的任意一个绝对位置
            in.seek(0);
            IOUtils.copyBytes(in, os, 4096, false);
        }finally {
            IOUtils.closeStream(os);
            IOUtils.closeStream(in);
        }

//        try{
//            //在已有文件末尾追加数据, LocalFileSystem并未实现该方法
//            os1 = fileSystem.append(new Path("file:/Users/xiyongchun/workspace/canary/scalaDemo/words.txt"));
//            os1.write("dfdsfdsdfs".getBytes());
//        }finally {
//            IOUtils.closeStream(os1);
//        }
    }

    public static void makeDirAndGetFileStatus() throws IOException{
        String uri = "file:/Users/xiyongchun/workspace/canary/scalaDemo/";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);
        System.out.println(fs.mkdirs(new Path(uri+"/test/test")));

        FileStatus fileStatus = fs.getFileStatus(new Path(uri+"/test/test"));
        System.out.println(fileStatus.getPath());
        System.out.println(fileStatus.isDirectory());

        FileStatus[] fileStatusList = fs.listStatus(new Path(uri+"/test"));
        Arrays.asList(fileStatusList).forEach(fileStatusItem->{
            System.out.println(fileStatusItem.getPath());
            System.out.println(fileStatusItem.isDirectory());
        });
    }

}
