package com.youyu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
 * https://www.iteblog.com/archives/881.html
 * Created by root on 2017/6/13.
 */
public class AppendHDFSContent {
    public static void main(String[] args) {
        String hdfs_path = "hdfs://192.168.1.46/user/gaoht/data/f1.txt";//文件路径
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);

        String inpath = "/app/bi/gaoht/learn/file/f1.txt";
        FileSystem fs = null;

        try {
            fs = FileSystem.get(URI.create(hdfs_path), conf);

            //要追加的文件流,inpath为文件
            InputStream in = new BufferedInputStream(new FileInputStream(inpath));

            OutputStream out = fs.append(new Path(hdfs_path));

            IOUtils.copyBytes(in, out, 4096, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
