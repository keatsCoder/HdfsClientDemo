package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

/**
 * 配置文件优先级演示
 */
public class ConfigFileTest {
    static FileSystem fs;

    static {
        Configuration conf = new Configuration();
        // 测试硬编码配置，请打开此处注释
//        conf.set("dfs.replication", "2");
        try {
            fs = FileSystem.get(new URI("hdfs://linux102:9000"), conf, "root");
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        // 执行操作
        // 上传文件
        uploadFile();

        // 释放资源
        fs.close();
    }

    private static void uploadFile() throws IOException {
        URL systemResource = ClassLoader.getSystemResource("zhangsan.txt");
        String path = systemResource.getPath();

        fs.copyFromLocalFile(new Path(path), new Path("/three.txt"));
        // 测试 resources 请打开 hdfs.site.xml 的注释部分
//        fs.copyFromLocalFile(new Path(path), new Path("/one.txt"));
        // 打开第 20 行注释，测试 conf 配置
//        fs.copyFromLocalFile(new Path(path), new Path("/two.txt"));
    }
}
