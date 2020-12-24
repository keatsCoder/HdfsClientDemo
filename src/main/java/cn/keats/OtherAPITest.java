package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

import java.io.IOException;
import java.net.URI;


/**
 * 除 上传、下载外一些其他的 API 测试
 */
public class OtherAPITest {
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
//        deleteFile();
        renameFile();

        // 释放资源
        fs.close();
    }

    /**
     * 文件重命名
     */
    private static void renameFile() throws IOException {

        String dstFileName = "wangwu.txt";
        HdfsClient2.uploadFile();
        deleteFile(dstFileName);
        // 目标文件不存在，则更名成功
        boolean rename = fs.rename(new Path("/john/keats/love/zhangsan.txt"), new Path("/john/keats/love/", dstFileName));
        Assert.assertTrue(rename);

        // 目标文件存在，则更名失败
        boolean renameButDstIsExist = fs.rename(new Path("/john/keats/love/zhangsan.txt"), new Path("/john/keats/love/", dstFileName));
        Assert.assertFalse(renameButDstIsExist);
    }

    /**
     * 文件删除
     */
    private static void deleteFile() throws IOException {
        // /john/keats 是文件夹目录，递归设置为 false 会报错 PathIsNotEmptyDirectoryException: ``/john/keats is non empty': Directory is not empty
//        fs.delete(new Path("/john/keats"), false);

        // 先上传，再删除
        HdfsClient2.uploadFile();
        fs.delete(new Path("/john/keats/love/zhangsan.txt"), true);
    }

    /**
     * 文件删除
     */
    private static void deleteFile(String fileName) throws IOException {
        // /john/keats 是文件夹目录，递归设置为 false 会报错 PathIsNotEmptyDirectoryException: ``/john/keats is non empty': Directory is not empty
//        fs.delete(new Path("/john/keats"), false);

        // 先上传，再删除
        HdfsClient2.uploadFile();
        fs.delete(new Path("/john/keats/love/", fileName), true);
    }


}
