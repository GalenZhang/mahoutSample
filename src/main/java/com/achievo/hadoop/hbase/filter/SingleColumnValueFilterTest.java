package com.achievo.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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
public class SingleColumnValueFilterTest
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
		put1.addColumn(columnFamily1.getBytes(), "title".getBytes(), "hadoop".getBytes());
		put1.addColumn(columnFamily1.getBytes(), "content".getBytes(), "hadoop is easy".getBytes());
		put1.addColumn(columnFamily2.getBytes(), "user".getBytes(), "admin".getBytes());
		put1.addColumn(columnFamily2.getBytes(), "time".getBytes(), "20150901".getBytes());
		list.add(put1);

		Put put2 = new Put(("row2").getBytes());
		put2.addColumn(columnFamily1.getBytes(), "title".getBytes(), "hbase".getBytes());
		put2.addColumn(columnFamily1.getBytes(), "content".getBytes(), "hbase is hard".getBytes());
		put2.addColumn(columnFamily2.getBytes(), "user".getBytes(), "ljt".getBytes());
		put2.addColumn(columnFamily2.getBytes(), "time".getBytes(), "20150902".getBytes());
		list.add(put2);

		Put put3 = new Put(("row3").getBytes());
		put3.addColumn(columnFamily1.getBytes(), "title".getBytes(), "hive".getBytes());
		put3.addColumn(columnFamily1.getBytes(), "content".getBytes(), "what's hive".getBytes());
		put3.addColumn(columnFamily2.getBytes(), "user".getBytes(), "ljt".getBytes());
		put3.addColumn(columnFamily2.getBytes(), "time".getBytes(), "20150903".getBytes());
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
	public void filterValue() throws IOException
	{
		Scan scan = new Scan();
		// 创建一个单列值过滤器 找到title列值等于hive行的数据
		Filter filter1 = new SingleColumnValueFilter(columnFamily1.getBytes(), "title".getBytes(), CompareOp.EQUAL,
				Bytes.toBytes("hive"));
		scan.setFilter(filter1);
		ResultScanner rs1 = table.getScanner(scan);
		for (Result result : rs1)
		{
			for (Cell cell : result.rawCells())
			{
				System.out.println("filter1:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + " : "
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		rs1.close();

		// 创建一个单列值过滤器 找到content列值包含is行的数据
		Filter filter2 = new SingleColumnValueFilter(columnFamily1.getBytes(), "content".getBytes(), CompareOp.EQUAL,
				new RegexStringComparator("is"));
		scan.setFilter(filter2);
		ResultScanner rs2 = table.getScanner(scan);
		for (Result result : rs2)
		{
			for (Cell cell : result.rawCells())
			{
				System.out.println("filter2:" + Bytes.toString(CellUtil.cloneQualifier(cell)) + " : "
						+ Bytes.toString(CellUtil.cloneValue(cell)));
			}
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