package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

/**
 * 从 HDFS 下载文件
 */
public class DownLoadFileTest {
    static FileSystem fs;

    static {
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(new URI("hdfs://linux102:9000"), conf, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        // 执行操作
        downLoadFile();

        // 释放资源
        fs.close();
    }

    private static void downLoadFile() throws IOException {
        // 不删除，生成校验文件
//        fs.copyToLocalFile(new Path("/three.txt"), new Path("D://zhangsan.txt"));
        // 删除，不生成校验文件
        fs.copyToLocalFile(true,  new Path("/two.txt"), new Path("D://zhangsan.txt"), true);
    }
}
