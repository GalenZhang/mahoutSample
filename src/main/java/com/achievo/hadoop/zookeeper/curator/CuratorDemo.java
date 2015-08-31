package com.achievo.hadoop.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: CuratorDemo.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: CuratorDemo.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class CuratorDemo
{
	public static void main(String[] args)
	{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient("10.50.90.28:2181", retryPolicy);
		client.start();
		System.out.println("Connect zookeeper......");
		try
		{
			client.create().forPath("/home", "HomeData".getBytes());
			client.create().forPath("/home/path", "testData".getBytes());
			System.out.println("Create node success.");

			System.out.println("Node data /home/path: " + new String(client.getData().forPath("/home/path")));
			System.out.println("ls / => " + client.getChildren().forPath("/"));
			System.out.println("ls /home => " + client.getChildren().forPath("/home"));

			client.delete().forPath("/home/path");
			client.delete().forPath("/home");
			System.out.println("Delete node success.");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		client.close();
	}
}

/*
 * $Log: av-env.bat,v $
 */