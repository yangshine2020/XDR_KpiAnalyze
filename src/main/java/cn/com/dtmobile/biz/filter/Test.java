package cn.com.dtmobile.biz.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class Test {


    public static void main(String[] args) throws IOException, InterruptedException {
        Path hdfsPath = new Path("D:/classdata/input/s1mme/HEB_BD_MOBILE_CNOS_HUAWEI_CXDR_RNC032_0005_20200715163610_S1MME-145_0_0.tar.gz");
        Configuration conf = new Configuration();

//        conf.set("dfs.replication","2");
//        conf.set("fs.default.name", "hdfs://hadoop1:9000/");
        FileSystem fs = FileSystem.get(conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(hdfsPath);

        FSDataInputStream inputStream = fs.open(hdfsPath);
        BufferedReader reader = null;

        try {
            if (codec == null) {
                reader = new BufferedReader(new InputStreamReader(inputStream));
            } else {
                CompressionInputStream comInputStream = codec.createInputStream(inputStream);
                reader = new BufferedReader(new InputStreamReader(comInputStream));
            }
            System.out.println(reader.readLine().toString());

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            inputStream.close();
            reader.close();
        }


    }




}
