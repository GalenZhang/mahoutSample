package com.achievo.hadoop.zookeeper.demo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: QueueDemo.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://blog.fens.me/zookeeper-queue/
 * 
 *  Notes:
 * 	$Id: QueueDemo.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class QueueDemo
{
	public static void doOne() throws IOException, KeeperException, InterruptedException
	{
		String host1 = "10.50.90.54:2181";
		ZooKeeper zk = connection(host1);
		initQueue(zk);
		joinQueue(zk, 1);
		joinQueue(zk, 2);
		joinQueue(zk, 3);
		zk.close();
	}

	/**
	 * 
	 * 创建一个与服务器的连接.
	 *
	 * @param host
	 * @return
	 * @throws IOException
	 */
	public static ZooKeeper connection(String host) throws IOException
	{
		ZooKeeper zk = new ZooKeeper(host, 60000, new Watcher()
		{
			// 监控所有被触发的事件
			public void process(WatchedEvent event)
			{
				if (event.getPath() != null && event.getPath().equals("/event/start")
						&& event.getType() == Event.EventType.NodeCreated)
				{
					System.out.println("Queue has Completed. Finish testing!!!");
				}
			}

		});

		return zk;
	}

	/**
	 * 
	 * 初始化队列.
	 *
	 * @param zk
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void initQueue(ZooKeeper zk) throws KeeperException, InterruptedException
	{
		System.out.println("WATCH => /queue/start");
		zk.exists("/queue/start", true);

		if (zk.exists("/queue", false) == null)
		{
			System.out.println("create /queue task-queue");
			zk.create("/queue", "task-queue".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		else
		{
			System.out.println("/queue is exist!");
		}
	}

	/**
	 * 
	 * 增加队列节点.
	 *
	 * @param zk
	 * @param x
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void joinQueue(ZooKeeper zk, int x) throws KeeperException, InterruptedException
	{
		System.out.println("create /queue/x" + x + " x" + x);
		zk.create("/queue/x" + x, ("x" + x).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		isCompleted(zk);
	}

	/**
	 * 
	 * 检查队列是否完整.
	 *
	 * @param zk
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public static void isCompleted(ZooKeeper zk) throws KeeperException, InterruptedException
	{
		int size = 3;
		int length = zk.getChildren("/queue", true).size();
		System.out.println("Queue Complete:" + length + "/" + size);

		if (length >= size)
		{
			System.out.println("create /queue/start start");
			zk.create("/queue/start", "start".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
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
				joinQueue(zk, 1);
				break;
			case 2:
				zk = connection(host2);
				initQueue(zk);
				joinQueue(zk, 2);
				break;
			case 3:
				zk = connection(host3);
				initQueue(zk);
				joinQueue(zk, 3);
				break;
		}
	}

	public static void main(String[] args) throws Exception
	{
		// doOne();
		doAction(1);
		doAction(3);
		doAction(2);
	}
}

/*
 * $Log: av-env.bat,v $
 */