package cn.keats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;


/**
 * 除 上传、下载外一些其他的 API 测试
 */
public class OtherAPITest {
    static FileSystem fs;
    static Configuration conf;

    static {
        conf = new Configuration();
        try {
            fs = FileSystem.get(new URI("hdfs://linux102:9000"), conf, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        // 执行操作
//        deleteFile();
//        renameFile();
//        listFiles();
//        isFile();
//        copyFileFromDiskByIO();
//        copyFileFromHDFSByIO();
        copyFileSeek();

        // 释放资源
        fs.close();
    }

    /**
     * 从某个位置开始拷贝文件，用于读取某个完整文件的部分内容
     */
    public static void copyFileSeek() throws Exception{
        // 2 打开输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.10.1.tar.gz"));

        // 3 定位输入数据位置
        fis.seek(1024*1024*128);

        // 4 创建输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, conf);
    }

    /**
     * 从HDFS通过IO流下载内容到本地
     */
    public static void copyFileFromHDFSByIO() throws IOException {
        FSDataInputStream fis = fs.open(new Path("/zhangsan.txt"));

        // 3 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("D:/zhangsan1.txt"));

        // 4 流的对拷
        IOUtils.copyBytes(fis, fos, conf);
    }

    public static void copyFileFromDiskByIO() throws IOException {
        // 2 创建输入流
        FileInputStream fis = new FileInputStream(new File("D:/zhangsan.txt"));

        // 3 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/zhangsan.txt"));

        // 4 流对拷
        IOUtils.copyBytes(fis, fos, conf);
    }

    /**
     * 判断某地址下的内容是否为文件
     */
    public static void isFile() throws IOException {
//        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        FileStatus[] listStatus = fs.listStatus(new Path("/three.txt"));

        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }
    }

    /**
     * 文件的详情
     */
    public static void listFiles() throws IOException {
        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());
            // 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());

            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {

                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("-----------分割线----------");
        }
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
