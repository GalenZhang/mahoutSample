package com.achievo.hadoop.zookeeper.curator.locking;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: InterProcessMutexDemo.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: InterProcessMutexDemo.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class InterProcessMutexDemo
{
	public static void main(String[] args)
	{
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		final CuratorFramework client = CuratorFrameworkFactory.builder().connectString("10.50.90.28:2181")
				.sessionTimeoutMs(5000).connectionTimeoutMs(10000).retryPolicy(retryPolicy).namespace("text").build();
		client.start();

		InterProcessMutex lock = new InterProcessMutex(client, "/lock");
		try
		{
			lock.acquire();
			System.err.println("生成订单号");
			Thread.sleep(5000L);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				lock.release();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		try
		{
			lock.acquire();
			System.err.println("生成订单号");
			Thread.sleep(Long.MAX_VALUE);
		}
		catch (Exception e)
		{
		}
		finally
		{
			try
			{
				lock.release();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}

		client.close();
	}
}

/*
 * $Log: av-env.bat,v $
 */