package com.achievo.hadoop.zookeeper.curator.counter;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: DistributedAtomicLongExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: DistributedAtomicLongExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 2, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class DistributedAtomicLongExample
{
	private static final int QTY = 5;

	private static final String PATH = "/examples/counter";

	public static void main(String[] args)
	{
		String zkConnString = "127.0.0.1:2181";
		CuratorFramework client = null;
		try
		{
			client = CuratorFrameworkFactory.newClient(zkConnString, new ExponentialBackoffRetry(1000, 3));
			client.start();

			List<DistributedAtomicLong> examples = Lists.newArrayList();
			ExecutorService service = Executors.newFixedThreadPool(QTY);
			for (int i = 0; i < QTY; i++)
			{
				final DistributedAtomicLong count = new DistributedAtomicLong(client, PATH, new RetryNTimes(10, 10));
				examples.add(count);

				Callable<Void> task = new Callable<Void>()
				{
					@Override
					public Void call() throws Exception
					{
						try
						{
							Thread.sleep(1000);
							AtomicValue<Long> value = count.increment();
							// AtomicValue<Long> value = count.decrement();
							// AtomicValue<Long> value = count.add(100l);
							System.out.println("succeed: " + value.succeeded());
							if (value.succeeded())
							{
								System.out.println("Increment: form " + value.preValue() + " to " + value.postValue());
							}
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}
						return null;
					}
				};
				service.submit(task);
			}

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