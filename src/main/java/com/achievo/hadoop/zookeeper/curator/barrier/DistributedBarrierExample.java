package com.achievo.hadoop.zookeeper.curator.barrier;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: DistributedBarrierExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: DistributedBarrierExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class DistributedBarrierExample
{
	private static final int QTY = 5;

	private static final String PATH = "/examples/barrier";

	public static void main(String[] args)
	{
		String zkConnString = "127.0.0.1:2181";
		CuratorFramework client = null;
		try
		{
			client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 3));
			client.start();

			ExecutorService service = Executors.newFixedThreadPool(QTY);
			DistributedBarrier controlBarrier = new DistributedBarrier(client, PATH);
			controlBarrier.setBarrier();

			for (int i = 0; i < QTY; i++)
			{
				final DistributedBarrier barrier = new DistributedBarrier(client, PATH);
				final int index = i;
				Callable<Void> task = new Callable<Void>()
				{
					@Override
					public Void call() throws Exception
					{
						Thread.sleep((long) (3 * Math.random()));
						System.out.println("Client #" + index + " waits on Barrier");
						barrier.waitOnBarrier();
						System.out.println("Client #" + index + " begins");
						return null;
					}
				};
				service.submit(task);
			}

			Thread.sleep(10000);
			System.out.println("all Barrier instances should wait the condition");

			controlBarrier.removeBarrier();

			service.shutdown();
			service.awaitTermination(10, TimeUnit.MINUTES);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			CloseableUtils.closeQuietly(client);
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */