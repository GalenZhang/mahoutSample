package com.achievo.hadoop.zookeeper.demo;

import java.io.IOException;

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
 *  File: BasicDemo1.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://blog.fens.me/hadoop-zookeeper-intro/
 * 
 *  Notes:
 * 	$Id: BasicDemo1.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class BasicDemo1
{
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException
	{
		// 创建一个与服务器的连接
		ZooKeeper zk = new ZooKeeper("10.50.90.54:2181", 60000, new Watcher()
		{
			// 监控所有被触发的事件
			public void process(WatchedEvent event)
			{
				System.out.println("EVENT:" + event.getType());
			}
		});

		// 查看根节点
		System.out.println("ls / => " + zk.getChildren("/", true));

		// 创建一个目录节点
		if (zk.exists("/node", true) == null)
		{
			zk.create("/node", "conan".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("create /node conan");
			// 查看/node节点数据
			System.out.println("get /node => " + new String(zk.getData("/node", false, null)));
			// 查看根节点
			System.out.println("ls / => " + zk.getChildren("/", true));
		}

		// 创建一个子目录节点
		if (zk.exists("/node/sub1", true) == null)
		{
			zk.create("/node/sub1", "sub1".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("create /node/sub1 sub1");
			// 查看node节点
			System.out.println("ls /node => " + zk.getChildren("/node", true));
		}

		// 修改节点数据
		if (zk.exists("/node", true) != null)
		{
			zk.setData("/node", "changed".getBytes(), -1);
			// 查看/node节点数据
			System.out.println("get /node => " + new String(zk.getData("/node", false, null)));
		}

		// 删除节点
		if (zk.exists("/node/sub1", true) != null)
		{
			zk.delete("/node/sub1", -1);
			zk.delete("/node", -1);
			// 查看根节点
			System.out.println("ls / => " + zk.getChildren("/", true));
		}

		// 关闭连接
		zk.close();
	}
}

/*
 * $Log: av-env.bat,v $
 */