package com.achievo.hadoop.zookeeper.message;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: ZooKeeperJob.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: ZooKeeperJob.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class ZooKeeperJob
{
	public final static String QUEUE = "/queue";

	public final static String PROFIT = "/queue/profit";

	public final static String PURCHASE = "/queue/purchase";

	public final static String SELL = "/queue/sell";

	public final static String OTHER = "/queue/other";

	public static void main(String[] args) throws ClassNotFoundException, IOException, KeeperException,
			InterruptedException
	{
		doAction(2);
	}

	public static void doAction(int client) throws IOException, KeeperException, InterruptedException,
			ClassNotFoundException
	{
		String host1 = "10.50.90.54:2181";
		String host2 = "10.50.90.54:2182";
		String host3 = "10.50.90.54:2183";

		ZooKeeper zk = null;
		switch (client)
		{
			case 1:
				zk = connection(host1);
				initQueue(zk);
				doPurchase(zk);
				break;
			case 2:
				zk = connection(host2);
				initQueue(zk);
				doSell(zk);
				break;
			case 3:
				zk = connection(host3);
				initQueue(zk);
				doOther(zk);
				break;
		}
	}

	public static void doOther(ZooKeeper zk) throws KeeperException, InterruptedException, IOException
	{
		if (zk.exists(OTHER, false) == null)
		{
			Other.calcOther(Other.file);
			
			System.out.println();
			zk.create(OTHER, OTHER.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			System.out.println(OTHER + " is exists!");
		}
		isCompleted(zk);
	}

	public static void doSell(ZooKeeper zk) throws ClassNotFoundException, KeeperException, InterruptedException,
			IOException
	{
		if (zk.exists(SELL, false) == null)
		{
			Sell.run(Sell.path());

			System.out.println("create " + SELL);
			zk.create(SELL, SELL.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			System.out.println(SELL + " is exists!");
		}
		isCompleted(zk);
	}

	public static void doPurchase(ZooKeeper zk) throws KeeperException, InterruptedException, ClassNotFoundException,
			IOException
	{
		if (zk.exists(PURCHASE, false) == null)
		{
			Purchase.run(Purchase.path());

			System.out.println("create " + PURCHASE);
			zk.create(PURCHASE, PURCHASE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			System.out.println(PURCHASE + " is exists!");
		}
		isCompleted(zk);
	}

	public static void isCompleted(ZooKeeper zk) throws KeeperException, InterruptedException, NumberFormatException,
			IOException
	{
		int size = 3;
		List<String> children = zk.getChildren(QUEUE, true);
		int length = children.size();

		System.out.println("Queue Complete: " + length + "/" + size);
		if (length >= size)
		{
			System.out.println("create " + PROFIT);
			Profit.profit();
			zk.create(PROFIT, PROFIT.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

			for (String child : children) // 清空节点
			{
				zk.delete(QUEUE + "/" + child, -1);
			}
		}
	}

	public static void initQueue(ZooKeeper zk) throws KeeperException, InterruptedException
	{
		System.out.println("WATCH => " + PROFIT);
		zk.exists(PROFIT, true);

		if (zk.exists(QUEUE, false) == null)
		{
			System.out.println("create " + QUEUE);
			zk.create(QUEUE, QUEUE.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			System.out.println(QUEUE + " is exists!");
		}
	}

	// 创建一个与服务器的连接
	public static ZooKeeper connection(String host) throws IOException
	{
		ZooKeeper zk = new ZooKeeper(host, 60000, new Watcher()
		{
			// 监控所有被触发的事件
			public void process(WatchedEvent event)
			{
				if (event.getType() == Event.EventType.NodeCreated && event.getPath().equals(PROFIT))
				{
					System.out.println("Queue has Completed!!!");
				}
			}
		});
		return zk;
	}
}

/*
 * $Log: av-env.bat,v $
 */