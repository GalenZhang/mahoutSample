package com.achievo.hadoop.zookeeper.demo;

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
 *  File: QueueDemo2.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://blog.fens.me/zookeeper-queue-fifo/
 * 
 *  Notes:
 * 	$Id: QueueDemo2.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class QueueDemo2
{
	public static void doOne() throws IOException, KeeperException, InterruptedException
	{
		String host1 = "10.50.90.54:2181";
		ZooKeeper zk = connection(host1);
		initQueue(zk);
		produce(zk, 1);
		produce(zk, 2);
		cosume(zk);
		cosume(zk);
		cosume(zk);
		zk.close();
	}

	public static ZooKeeper connection(String host) throws IOException
	{
		ZooKeeper zk = new ZooKeeper(host, 60000, new Watcher()
		{

			public void process(WatchedEvent arg0)
			{

			}

		});

		return zk;
	}

	public static void initQueue(ZooKeeper zk) throws KeeperException, InterruptedException
	{
		if (zk.exists("/queue-fifo", false) == null)
		{
			System.out.println("create /queue-fifo task-queue-fifo");
			zk.create("/queue-fifo", "task-queue-fifo".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			System.out.println("/queue-fifo is exist!");
		}
	}
	
	public static void produce(ZooKeeper zk, int x) throws KeeperException, InterruptedException
	{
		System.out.println("create /queue-fifo/x" + x + " x" + x);
		zk.create("/queue-fifo/x" + x, ("x" + x).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	public static void cosume(ZooKeeper zk) throws KeeperException, InterruptedException
	{
		List<String> list = zk.getChildren("/queue-fifo", true);
		if (list.size() > 0)
		{
			Long min = Long.MAX_VALUE;
			for (String num : list)
			{
				System.out.println("num: " + num);
				if (min > Long.parseLong(num.substring(1)))
				{
					min = Long.parseLong(num.substring(1));
				}
			}
			System.out.println("delete /queue/x" + min);
			zk.delete("/queue-fifo/x" + min, 0);
		}
		else
		{
			System.out.println("No node to cosume");
		}
	}
	
	public static void doAction(int client) throws IOException, KeeperException, InterruptedException
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
				produce(zk, 1);
				break;
			case 2:
				zk = connection(host2);
				initQueue(zk);
				produce(zk, 2);
				break;
			case 3:
				zk = connection(host3);
				initQueue(zk);
				cosume(zk);
				cosume(zk);
				cosume(zk);
				break;	
		}
	}
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException
	{
//		doOne();
		doAction(1);
		doAction(2);
		doAction(3);
	}
}

/*
 * $Log: av-env.bat,v $
 */