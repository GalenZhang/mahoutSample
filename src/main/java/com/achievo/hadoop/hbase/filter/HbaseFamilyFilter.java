package com.achievo.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HbaseFilter.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://coriger.com/article/html/88
 * 
 *  Notes:
 * 	$Id: HbaseFilter.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 21, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HbaseFamilyFilter
{
	private Configuration conf;

	private Admin admin;

	private Table table;

	private TableName tn;

	private String tableName = "t_article";

	private String columnFamily1 = "info";

	private String columnFamily2 = "comment";

	private Connection conn;

	@Before
	public void init()
	{
		conf = HBaseConfiguration.create();
		try
		{
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
			tn = TableName.valueOf(tableName);
			table = conn.getTable(tn);
			initData();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 初始化数据
	 */
	public void initData()
	{
		try
		{
			if (admin.tableExists(tn))
			{
				System.out.println(tableName + " exists !");
			}
			else
			{
				HTableDescriptor desc = new HTableDescriptor(tn);
				for (int i = 0; i < 3; i++)
				{
					desc.addFamily(new HColumnDescriptor(columnFamily1 + i));
					desc.addFamily(new HColumnDescriptor(columnFamily2 + i));
				}
				admin.createTable(desc);
				System.out.println("create " + tableName + " success !");
				addData();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public void addData() throws IOException
	{
		List<Put> list = new ArrayList<Put>();
		for (int i = 0; i < 5; i++)
		{
			Put put = new Put(("row" + i).getBytes());
			for (int j = 0; j < 3; j++)
			{
				put.addColumn((columnFamily1 + j).getBytes(), "title".getBytes(), ("title" + i).getBytes());
				put.addColumn((columnFamily1 + j).getBytes(), "content".getBytes(), ("content" + i).getBytes());
				put.addColumn((columnFamily2 + j).getBytes(), "user".getBytes(), ("user" + i).getBytes());
				put.addColumn((columnFamily2 + j).getBytes(), "time".getBytes(), ("time" + i).getBytes());
			}

			list.add(put);
		}
		table.put(list);
		System.out.println("data add success!");
	}

	/**
	 * 过滤行键
	 * 
	 * @throws IOException
	 */
	@Test
	public void filterColumnFamily() throws IOException
	{
		Scan scan = new Scan();
		scan.addColumn(columnFamily1.getBytes(), "title".getBytes());
		// 创建一个列族过滤器 列族名等于info1
		Filter filter1 = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info1")));
		scan.setFilter(filter1);
		ResultScanner rs1 = table.getScanner(scan);
		for (Result result : rs1)
		{
			System.out.println("filter1:" + result);
		}
		rs1.close();

		Get get1 = new Get(Bytes.toBytes("row3"));
		Filter filter2 = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("comment2")));
		get1.setFilter(filter2);
		Result result = table.get(get1);
		System.out.println("get1: " + result);

		Get get2 = new Get(Bytes.toBytes("row3"));
		get2.addFamily(Bytes.toBytes("coment1"));
		Filter filter3 = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("info2")));
		get2.setFilter(filter3);
		Result result2 = table.get(get2);
		System.out.println("get2: " + result2);
	}

	@After
	public void close() throws IOException
	{
		deleteTable();
		admin.close();
		conn.close();
	}

	private void deleteTable() throws IOException
	{
		admin.disableTable(tn);
		admin.deleteTable(tn);
		if (!admin.tableExists(tn))
		{
			System.out.println(tableName + " is delete !");
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */