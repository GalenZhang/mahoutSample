package com.achievo.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HdfsApi.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://fromwiz.com/share/s/09liu01n_N7y2FJV1W2R6SqK1Hz_4f3s7Qoq2wJ9Ta1292Q3
 * 
 *  Notes:
 * 	$Id: HdfsApi.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 18, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HdfsApi
{
	/** HDFS服务访问地址 **/
	public static final String HDFS_PATH = "hdfs://10.50.90.28:9000";

	private static FileSystem fileSystem;

	private static DistributedFileSystem hdfs;

	@Before
	public void init()
	{
		try
		{
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", HDFS_PATH);
			fileSystem = FileSystem.get(conf);
			hdfs = (DistributedFileSystem) fileSystem;
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 创建目录
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void mkdir() throws IllegalArgumentException, IOException
	{
		if (!fileSystem.exists(new Path("/test")))
		{
			boolean flag = fileSystem.mkdirs(new Path("/test"));
			if (flag)
			{
				System.out.println("创建目录成功...");
			}
			else
			{
				System.out.println("创建目录失败...");
			}
		}
		else
		{
			System.out.println("目录已存在...");
		}
	}

	/**
	 * 创建文件
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void createFile() throws IllegalArgumentException, IOException
	{
		FSDataOutputStream os = fileSystem.create(new Path("/test/test1.txt"));
		os.write("Hello world! Hello hadoop!".getBytes());
		os.close();
		System.out.println("创建文件成功...");
	}

	/**
	 * 上传本地文件到hdfs中
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void updateFile() throws IllegalArgumentException, IOException
	{
		File file = new File("/home/hadoop/temp/data.txt");

		FileInputStream is = new FileInputStream(file);
		InputStreamReader isr = new InputStreamReader(is, "utf-8");
		BufferedReader br = new BufferedReader(isr);

		FSDataOutputStream os = fileSystem.create(new Path("/test/data.txt"));
		Writer out = new OutputStreamWriter(os, "utf-8");

		String str = "";
		while ((str = br.readLine()) != null)
		{
			out.write(str + "\n");
		}

		out.close();
		os.close();
		br.close();
		isr.close();
		is.close();
	}

	/**
	 * 读取文件内容
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void readFile() throws IllegalArgumentException, IOException
	{
		FSDataInputStream is = fileSystem.open(new Path("/test/data.txt"));
		InputStreamReader sr = new InputStreamReader(is, "utf-8");
		BufferedReader br = new BufferedReader(sr);
		String str = "";
		while ((str = br.readLine()) != null)
		{
			System.out.println(str);
		}
		br.close();
		sr.close();
		is.close();
	}

	/**
	 * 列出所有DataNode的名字信息
	 * 
	 * @throws IOException
	 */
	@Test
	public void listDataNodeInfo() throws IOException
	{
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		String[] names = new String[dataNodeStats.length];
		for (int i = 0; i < names.length; i++)
		{
			names[i] = dataNodeStats[i].getHostName();
			System.out.println(names[i]);
		}
		System.out.println(hdfs.getUri().toString());
	}

	/**
	 * 取得文件块所在的位置..
	 * 
	 * @throws IOException
	 */
	@Test
	public void getLocation() throws IOException
	{
		Path f = new Path("/test/data.txt");
		FileStatus fileStatus = fileSystem.getFileStatus(f);

		BlockLocation[] blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		for (BlockLocation currentLocation : blkLocations)
		{
			String[] hosts = currentLocation.getHosts();
			for (String host : hosts)
			{
				System.out.println(host);
			}
		}

		// 取得最后修改时间
		long modifyTime = fileStatus.getModificationTime();
		Date d = new Date(modifyTime);
		System.out.println(d);
	}

	@Test
	public void listFileStatus() throws FileNotFoundException, IllegalArgumentException, IOException
	{
		FileStatus fileStatus[] = fileSystem.listStatus(new Path("/test"));
		int listLength = fileStatus.length;
		for (int i = 0; i < listLength; i++)
		{
			if (fileStatus[i].isDirectory() == false)
			{
				System.out.println("filename: " + fileStatus[i].getPath().getName() + "\tsize: "
						+ fileStatus[i].getLen());
			}
		}
	}

	/**
	 * 文件重命名
	 * 
	 * @throws IOException
	 */
	@Test
	public void rename() throws IOException
	{
		Path oldPath = new Path("/test");
		Path newPath = new Path("/newTest");
		fileSystem.rename(oldPath, newPath);
	}

	/**
	 * 删除文件或目录 级联删除
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	@Test
	public void delete() throws IllegalArgumentException, IOException
	{
		fileSystem.delete(new Path("/newTest"), true);
	}
}

/*
 * $Log: av-env.bat,v $
 */