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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
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
public class HbaseQualifierFilter
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
				desc.addFamily(new HColumnDescriptor(columnFamily1));
				desc.addFamily(new HColumnDescriptor(columnFamily2));
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
		Put put1 = new Put(("row1").getBytes());
		put1.addColumn(columnFamily1.getBytes(), "title".getBytes(), ("title").getBytes());
		put1.addColumn(columnFamily1.getBytes(), "content".getBytes(), ("content").getBytes());
		put1.addColumn(columnFamily2.getBytes(), "user".getBytes(), ("user").getBytes());
		put1.addColumn(columnFamily2.getBytes(), "time".getBytes(), ("time").getBytes());
		list.add(put1);

		Put put2 = new Put(("row2").getBytes());
		put2.addColumn(columnFamily1.getBytes(), "thumbUrl".getBytes(), ("title").getBytes());
		put2.addColumn(columnFamily1.getBytes(), "author".getBytes(), ("content").getBytes());
		put2.addColumn(columnFamily2.getBytes(), "age".getBytes(), ("user").getBytes());
		list.add(put2);

		Put put3 = new Put(("row2").getBytes());
		put3.addColumn(columnFamily1.getBytes(), "title".getBytes(), ("title").getBytes());
		put3.addColumn(columnFamily1.getBytes(), "author".getBytes(), ("content").getBytes());
		put3.addColumn(columnFamily2.getBytes(), "age".getBytes(), ("user").getBytes());
		put3.addColumn(columnFamily2.getBytes(), "time".getBytes(), ("time").getBytes());
		list.add(put3);

		table.put(list);
		System.out.println("data add success!");
	}

	/**
	 * 过滤行键
	 * 
	 * @throws IOException
	 */
	@Test
	public void filterQualifier() throws IOException
	{
		Scan scan = new Scan();
		scan.addColumn(columnFamily1.getBytes(), "title".getBytes());
		// 创建一个列过滤器 列名等于thumbUrl 应该打印出row2
		Filter filter1 = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("thumbUrl")));
		scan.setFilter(filter1);
		ResultScanner rs1 = table.getScanner(scan);
		for (Result result : rs1)
		{
			System.out.println("filter1:" + result);
		}
		rs1.close();

		Filter filter2 = new QualifierFilter(CompareOp.EQUAL, new SubstringComparator("a"));
		scan.setFilter(filter2);
		ResultScanner rs2 = table.getScanner(scan);
		for (Result result : rs2)
		{
			System.out.println("filter2:" + result);
		}
		rs2.close();
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