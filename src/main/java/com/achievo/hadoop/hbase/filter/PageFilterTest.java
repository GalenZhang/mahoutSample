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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
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
public class PageFilterTest
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
		for (int i = 0; i < 20; i++)
		{
			Put put = new Put(("row" + i).getBytes());
			put.addColumn(columnFamily1.getBytes(), "title".getBytes(), "hadoop".getBytes());
			put.addColumn(columnFamily1.getBytes(), "content".getBytes(), "hadoop is easy".getBytes());
			put.addColumn(columnFamily2.getBytes(), "user".getBytes(), "admin".getBytes());
			put.addColumn(columnFamily2.getBytes(), "time".getBytes(), "20150901".getBytes());
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
	public void filterValue() throws IOException
	{
		// 每页返回5行
		Filter filter = new PageFilter(5);
		final byte[] POSTFIX = new byte[] {0x00};
		byte[] lastRow = null;
		int totalRows = 0;
		int index = 0;
		while (true)
		{
			Scan scan = new Scan();
			scan.setFilter(filter);

			if (lastRow != null)
			{
				byte[] startRow = Bytes.add(lastRow, POSTFIX);
				scan.setStartRow(startRow);
			}

			ResultScanner rs = table.getScanner(scan);
			boolean flag = false;
			index++;
			for (Result result : rs)
			{
				lastRow = result.getRow();
				if (lastRow != null && lastRow.length > 0)
				{
					flag = true;
					totalRows++;
					System.out.println("第" + index + "次遍历结果：" + result);
				}
			}
			rs.close();

			if (!flag)
			{
				System.out.println("总共：" + totalRows + "行,遍历：" + (index - 1) + "次");
				break;
			}
		}

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