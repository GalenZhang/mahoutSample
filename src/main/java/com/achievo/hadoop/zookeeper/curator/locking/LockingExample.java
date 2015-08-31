package com.achievo.hadoop.zookeeper.curator.locking;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: LockingExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: LockingExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Aug 31, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class LockingExample
{
	private static final int QTY = 5;

	private static final int REPETITIONS = QTY * 10;

	private static final String PATH = "/examples/locks";

	public static void main(String[] args) throws Exception
	{
		final FakeLimitedResource resource = new FakeLimitedResource();

		ExecutorService service = Executors.newFixedThreadPool(QTY);
		final TestingServer server = new TestingServer();

		try
		{
			for (int i = 0; i < QTY; i++)
			{
				final int index = i;
				Callable<Void> task = new Callable<Void>()
				{
					public Void call() throws Exception
					{
						CuratorFramework client = CuratorFrameworkFactory.newClient("10.50.90.28:2181",
							new ExponentialBackoffRetry(1000, 3));
						try
						{
							client.start();

							ExampleClientThatLocks example = new ExampleClientThatLocks(client, PATH, resource,
									"Client " + index);
							for (int j = 0; j < REPETITIONS; j++)
							{
								example.doWork(10, TimeUnit.SECONDS);
							}
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}
						finally
						{
							CloseableUtils.closeQuietly(client);
						}
						return null;
					}
				};
				service.submit(task);
			}

			service.shutdown();
			service.awaitTermination(10, TimeUnit.MINUTES);
		}
		finally
		{
			CloseableUtils.closeQuietly(server);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */