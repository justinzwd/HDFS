package com.atguigu.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient
{
    private FileSystem fileSystem;
    @Before
    public void before() throws IOException, InterruptedException
    {
        System.out.println("Before!!!!!");
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
    }

    @Test
    public void put() throws IOException
    {
        fileSystem.copyFromLocalFile(new Path("d:\\1.txt"),new Path("/"));
    }

    @Test
    public void get() throws IOException, InterruptedException
    {
        //获取一个HDFS的抽象封装对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
        //用这个对象操作文件系统
        fileSystem.copyToLocalFile(new Path("/input"),new Path("d:\\"));
        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException
    {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "atguigu");
        fileSystem.rename(new Path("/input"), new Path("/input2"));
        fileSystem.close();
    }

    @Test
    public void delete() throws IOException
    {
        boolean delete = fileSystem.delete(new Path("/1.txt"), true);
        if (delete)
        {
            System.out.println("删除成功");
        }
        else
            System.out.println("删除失败");
    }

    @Test
    public void append() throws IOException
    {
        FSDataOutputStream append = fileSystem.append(new Path("/word.txt"), 1024);
        FileInputStream open = new FileInputStream("d:\\1.txt");
        IOUtils.copyBytes(open,append,1024,true);
    }

    @Test
    public void ls() throws IOException
    {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses)
        {
            if (fileStatus.isFile())
            {
                System.out.println("文件");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
                System.out.println(fileStatus.getPermission());
            }
            else
            {
                System.out.println("文件夹");
                System.out.println(fileStatus.getPath());
            }
        }
    }
    
    @Test
    public void listFile() throws IOException
    {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while(locatedFileStatusRemoteIterator.hasNext())
        {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println("------------------");
            System.out.println(next.getPath());

            System.out.println("块信息");
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations)
            {
                System.out.println("块在");
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts)
                {
                    System.out.println(host);
                }
            }
        }
    }
    
    @After
    public void after() throws IOException
    {
        System.out.println("After!!!!!");
        fileSystem.close();
    }
}
