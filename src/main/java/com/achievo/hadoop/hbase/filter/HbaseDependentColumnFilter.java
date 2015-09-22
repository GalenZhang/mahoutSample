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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
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
public class HbaseDependentColumnFilter
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
		list.add(put1);

		Put put2 = new Put(("row2").getBytes());
		put2.addColumn(columnFamily1.getBytes(), "content".getBytes(), "hbase is hard".getBytes());
		list.add(put2);

		Put put3 = new Put(("row3").getBytes());
		put3.addColumn(columnFamily1.getBytes(), "title".getBytes(), "hive".getBytes());
		put3.addColumn(columnFamily1.getBytes(), "content".getBytes(), "what's hive".getBytes());
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
		// 如果第三个参数是true则返回值中没有该列的值, 如果是false则有
		// 所以这里会返回所有有title列的值 但是不会返回title值
		Filter filter1 = new DependentColumnFilter(columnFamily1.getBytes(), "title".getBytes(), true);
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

		// 这里会返回有title列并且title列值等于hive的数据 并且会返回title值
		Filter filter2 = new DependentColumnFilter(columnFamily1.getBytes(), "title".getBytes(), false,
				CompareOp.EQUAL, new BinaryComparator("hard".getBytes()));
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