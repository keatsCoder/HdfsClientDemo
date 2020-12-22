package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

/**
 * 通过 get 重载方法 配置 fs 对象和用户名
 */
public class HdfsClient2 {
    static FileSystem fs;

    static {
        Configuration conf = new Configuration();
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

        fs.copyFromLocalFile(new Path(path), new Path("/john/keats/love"));
    }
}
