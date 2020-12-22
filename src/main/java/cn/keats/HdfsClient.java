package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 通过 conf 配置 fs 对象
 */
public class HdfsClient {
    static FileSystem fs;

    static {
        // 初始化 fs 对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://linux102:9000");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        // 执行操作
        mkDir();

        // 释放资源
        fs.close();
    }

    private static void mkDir() throws IOException {
        fs.mkdirs(new Path("/john/keats"));
    }
}
