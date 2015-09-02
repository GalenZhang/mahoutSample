package com.achievo.hadoop.zookeeper.curator.counter;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: SharedCounterExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SharedCounterExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 1, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class SharedCounterExample implements SharedCountListener
{
	private static final int QTY = 5;

	private static final String PATH = "/examples/counter";

	public static void main(String[] args)
	{
		String zkConnString = "127.0.0.1:2181";
		CuratorFramework client = null;
		final Random rand = new Random();
		SharedCounterExample example = new SharedCounterExample();

		try
		{
			client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 3));
			client.start();

			SharedCount baseCount = new SharedCount(client, PATH, 0);
			baseCount.addListener(example);
			baseCount.start();

			List<SharedCount> examples = Lists.newArrayList();
			ExecutorService service = Executors.newFixedThreadPool(QTY);

			for (int i = 0; i < QTY; i++)
			{
				final SharedCount count = new SharedCount(client, PATH, 0);
				examples.add(count);
				Callable<Void> task = new Callable<Void>()
				{
					@Override
					public Void call() throws Exception
					{
						count.start();
						Thread.sleep(rand.nextInt(10000));
						System.out.println("Increment: "
								+ count.trySetCount(count.getVersionedValue(), count.getCount() + rand.nextInt(10)));
						return null;
					}
				};
				service.submit(task);
			}

			service.shutdown();
			service.awaitTermination(10, TimeUnit.MINUTES);

			for (int i = 0; i < QTY; i++)
			{
				examples.get(i).close();
			}
			baseCount.close();
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

	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState)
	{
		System.out.println("State changed: " + newState.toString());
	}

	@Override
	public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception
	{
		System.out.println("Counter's value is changed to " + newCount);
	}
}

/*
 * $Log: av-env.bat,v $
 */