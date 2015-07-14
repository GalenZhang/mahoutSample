package com.achievo.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Test;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HBaseTest.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: HBaseTest.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HBaseTest
{
	private static Configuration conf = null;

	static
	{
		conf = HBaseConfiguration.create();
		conf.set("hbase.ZooKeeper.property.clientPort", "2181");
		conf.set("hbase.ZooKeeper.quorum", "10.50.90.54");
		conf.set("hbase.master", "10.50.90.54:60000");
	}

	public static void createTable() throws Exception
	{
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("user");

		// 创建 user 表
		HTableDescriptor tableDescr = new HTableDescriptor(tableName);
		tableDescr.addFamily(new HColumnDescriptor("phone".getBytes()));
		tableDescr.addFamily(new HColumnDescriptor("info".getBytes()));
		System.out.println("Creating table `user`. ");

		if (admin.tableExists(tableName))
		{
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		admin.createTable(tableDescr);
		System.out.println("Done!");
	}

	@Test
	public void testHBase() throws Exception
	{
		HBaseTest.createTable();
	}
}

/*
 * $Log: av-env.bat,v $
 */