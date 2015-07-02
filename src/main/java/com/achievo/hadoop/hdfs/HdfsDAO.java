package com.achievo.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HdfsDAO.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  https://github.com/bsspirit/maven_mahout_template/blob/mahout-0.8/src/main/java/org/conan/mymahout/hdfs/HdfsDAO.java
 * 
 *  Notes:
 * 	$Id: HdfsDAO.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 2, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HdfsDAO
{
	private static final String HDFS = "hdfs://localhost:9000/";

	private String hdfsPath;

	private Configuration conf;

	public HdfsDAO(Configuration conf)
	{
		this(HDFS, conf);
	}

	public HdfsDAO(String hdfs, Configuration conf)
	{
		this.hdfsPath = hdfs;
		this.conf = conf;
	}

	public void copyFile(String local, String remote) throws IOException
	{
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from: " + local + " to " + remote);
	}

	public void ls(String folder) throws IOException
	{
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		System.out.println("==========================================================");
		for (FileStatus f : list)
		{
			System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDirectory(), f.getLen());
		}
		System.out.println("==========================================================");
		fs.close();
	}

	public static void main(String[] args) throws IOException
	{
		Configuration conf = config();
		HdfsDAO hdfs = new HdfsDAO(conf);
		hdfs.copyFile(HdfsDAO.class.getClassLoader().getResource("").getPath() + "datafile/item.csv", "/tmp/new");
		hdfs.ls("/tmp/new");
	}

	public static Configuration config()
	{
		Configuration conf = new Configuration();
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}
}

/*
 * $Log: av-env.bat,v $
 */