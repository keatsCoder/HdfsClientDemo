package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

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
//        batchUploadFile();
        // 释放资源
        fs.close();
    }

    public static void uploadFile() throws IOException {
        URL systemResource = ClassLoader.getSystemResource("zhangsan.txt");
        String path = systemResource.getPath();

        fs.copyFromLocalFile(new Path(path), new Path("/john/keats/love"));
    }

    /**
     * 多文件上传，注意目标路径须是文件夹路径
     */
    public static void batchUploadFile() throws IOException {
        URL systemResource = ClassLoader.getSystemResource("zhangsan.txt");
        URL systemResource2 = ClassLoader.getSystemResource("luoxiang.txt");
        String path = systemResource.getPath();
        String path2 = systemResource2.getPath();

        fs.copyFromLocalFile(false, true, new Path[]{new Path(path), new Path(path2)}, new Path("/john/keats/love/"));
    }
}
