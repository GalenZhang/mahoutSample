package com.achievo.hadoop.hbase;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
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
 *  http://wuchong.me/blog/2015/04/06/spark-on-hbase-new-api/
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
		admin.close();
		connection.close();
	}

	public static void putRow() throws Exception
	{
		Connection connection = ConnectionFactory.createConnection(conf);
		TableName tableName = TableName.valueOf("user");
		Table table = connection.getTable(tableName);
		// 准备插入一条 key 为 1 的数据
		Put p = new Put("1".getBytes());
		// 为put操作指定 column 和 value （以前的 put.add 方法被弃用了）
		p.addColumn("info".getBytes(), "name".getBytes(), "wuchong".getBytes());
		table.put(p);

		// 查询某条数据
		Get g = new Get("1".getBytes());
		Result result = table.get(g);
		String value = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()));
		System.out.println("Get id 1: " + value);
		outputResult(result);

		// 扫描数据
		Scan s = new Scan();
		s.addColumn("info".getBytes(), "name".getBytes());
		ResultScanner scanner = table.getScanner(s);
		try
		{
			for (Result rs : scanner)
			{
				outputResult(rs);
			}
		}
		finally
		{
			// 确保scanner关闭
			scanner.close();
		}

		// 删除某条数据,操作方式与 Put 类似
		Delete d = new Delete("1".getBytes());
		d.addColumn("info".getBytes(), "name".getBytes());
		table.delete(d);

		table.close();
		connection.close();
	}

	public static void outputResult(Result rs)
	{
		List<Cell> list = rs.listCells();
		System.out.println("row key: " + new String(rs.getRow()));
		for (Cell cell : list)
		{
			System.out.println("family: " + new String(CellUtil.cloneFamily(cell)) + ", col: "
					+ new String(CellUtil.cloneQualifier(cell)) + ", value: " + new String(CellUtil.cloneValue(cell)));
		}
	}

	@Test
	public void testHBase() throws Exception
	{
		HBaseTest.createTable();
		HBaseTest.putRow();
	}
}

/*
 * $Log: av-env.bat,v $
 */