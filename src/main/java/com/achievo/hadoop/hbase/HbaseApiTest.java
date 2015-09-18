package com.achievo.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HbaseApiTest.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://coriger.com/article/html/86
 * 
 *  Notes:
 * 	$Id: HbaseApiTest.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 18, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HbaseApiTest
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
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 创建表
	 */
	@Test
	public void createTable()
	{
		try
		{
			if (admin.tableExists(tn))
			{
				System.out.println(tableName + " exists !");
			}
			else
			{
				// 表描述对象
				HTableDescriptor desc = new HTableDescriptor(tn);
				// 添加了两个列族 分别是info、comment
				desc.addFamily(new HColumnDescriptor(columnFamily1));
				desc.addFamily(new HColumnDescriptor(columnFamily2));
				// 创建表
				admin.createTable(desc);
				System.out.println("create " + tableName + " success !");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 列出表
	 */
	@Test
	public void listTable()
	{
		try
		{
			// 获取所有的表
			HTableDescriptor[] descs = admin.listTables();
			for (HTableDescriptor desc : descs)
			{
				System.out.println(desc.getTableName());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 禁用一个表
	 */
	@Test
	public void disableTable()
	{
		try
		{
			admin.disableTable(tn);
			if (admin.isTableDisabled(tn))
			{
				System.out.println(tableName + " disabled !");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 启用一个表
	 */
	@Test
	public void enableTable()
	{
		try
		{
			admin.enableTable(tn);
			if (admin.isTableAvailable(tn))
			{
				System.out.println(tableName + " enabled !");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 查询所有列族
	 */
	@Test
	public void queryColumnFamily()
	{
		try
		{
			// 获取列族描述数组
			HColumnDescriptor[] hColumnDescriptors = table.getTableDescriptor().getColumnFamilies();
			for (HColumnDescriptor desc : hColumnDescriptors)
			{
				System.out.println(desc.getNameAsString());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 新增列族
	 */
	@Test
	public void addColumnFamily()
	{
		HColumnDescriptor descriptor = new HColumnDescriptor("addColumnFam");
		try
		{
			admin.addColumn(tn, descriptor);
			System.out.println("add " + descriptor.getNameAsString() + " success");
			// 查询所有列族列表
			queryColumnFamily();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 删除列族
	 */
	@Test
	public void deleteColumnFamily()
	{
		try
		{
			admin.deleteColumn(tn, "addColumnFam".getBytes());
			System.out.println("delete addColumnFam success");
			// 查询所有列族列表
			queryColumnFamily();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 新增数据
	 */
	@Test
	public void addData()
	{
		// 设置rowkey
		Put put = new Put("row1".getBytes());

		// 大致就是这个文章的标题是title1,内容是content1,评论人是user,评论时间是20150907
		put.addColumn(columnFamily1.getBytes(), "title".getBytes(), "title1".getBytes());
		put.addColumn(columnFamily1.getBytes(), "content".getBytes(), "test content".getBytes());
		put.addColumn(columnFamily2.getBytes(), "user".getBytes(), "ZhangSan".getBytes());
		put.addColumn(columnFamily2.getBytes(), "time".getBytes(), "20150907".getBytes());

		try
		{
			table.put(put);
			System.out.println("data add success!");
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 查询行数据
	 */
	@Test
	public void queryRowData()
	{
		// 设置rowkey 和上面插入的一致
		Get get = new Get("row1".getBytes());
		try
		{
			Result result = table.get(get);

			String title = Bytes.toString(result.getValue(columnFamily1.getBytes(), "title".getBytes()));
			String content = Bytes.toString(result.getValue(columnFamily1.getBytes(), "content".getBytes()));
			String user = Bytes.toString(result.getValue(columnFamily2.getBytes(), "user".getBytes()));
			String time = Bytes.toString(result.getValue(columnFamily2.getBytes(), "time".getBytes()));

			System.out.println("title is :" + title);
			System.out.println("content is :" + content);
			System.out.println("user is :" + user);
			System.out.println("time is :" + time);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 扫描数据
	 */
	@Test
	public void scan()
	{
		Scan scan = new Scan();
		// 查询指定列族
		// scan.addColumn(columnFamily1.getBytes(), "title".getBytes());
		scan.addFamily(columnFamily1.getBytes());
		try
		{
			ResultScanner rs = table.getScanner(scan);
			for (Result result : rs)
			{
				System.out.println(result.toString());
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 查询列族数据
	 */
	@Test
	public void queryFamilyData()
	{
		// 设置rowkey 和上面插入的一致
		Get get = new Get("row1".getBytes());
		// 只获取列族info下的列数据
		get.addFamily(columnFamily1.getBytes());

		try
		{
			Result result = table.get(get);

			String title = Bytes.toString(result.getValue(columnFamily1.getBytes(), "title".getBytes()));
			String content = Bytes.toString(result.getValue(columnFamily1.getBytes(), "content".getBytes()));
			String user = Bytes.toString(result.getValue(columnFamily2.getBytes(), "user".getBytes()));
			String time = Bytes.toString(result.getValue(columnFamily2.getBytes(), "time".getBytes()));

			System.out.println("title is :" + title);
			System.out.println("content is :" + content);
			System.out.println("user is :" + user);
			System.out.println("time is :" + time);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 查询列数据
	 */
	@Test
	public void queryColumnData()
	{
		// 设置rowkey 和上面插入的一致
		Get get = new Get("row1".getBytes());
		// 只获取列族info下的列数据
		get.addColumn(columnFamily1.getBytes(), "tite".getBytes());

		try
		{
			Result result = table.get(get);

			String title = Bytes.toString(result.getValue(columnFamily1.getBytes(), "title".getBytes()));
			String content = Bytes.toString(result.getValue(columnFamily1.getBytes(), "content".getBytes()));
			String user = Bytes.toString(result.getValue(columnFamily2.getBytes(), "user".getBytes()));
			String time = Bytes.toString(result.getValue(columnFamily2.getBytes(), "time".getBytes()));

			System.out.println("title is :" + title);
			System.out.println("content is :" + content);
			System.out.println("user is :" + user);
			System.out.println("time is :" + time);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 更新列数据
	 */
	@Test
	public void updateColumnData()
	{
		Put put = new Put("row1".getBytes());
		put.addColumn(columnFamily1.getBytes(), "title".getBytes(), "title2".getBytes());
		try
		{
			table.put(put);
			System.out.println("data Updated");
			queryColumnData();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 删除列数据
	 */
	@Test
	public void deleteColumnData()
	{
		Delete delete = new Delete("row1".getBytes());
		delete.addColumn(columnFamily1.getBytes(), "title".getBytes());
		try
		{
			// 这里只是删除最新的版本
			table.delete(delete);
			System.out.println("data delete");
			queryColumnData();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 删除列族数据
	 */
	@Test
	public void deleteFamilyData()
	{
		Delete delete = new Delete("row1".getBytes());
		delete.addFamily(columnFamily1.getBytes());
		try
		{
			table.delete(delete);
			System.out.println("data delete");
			queryColumnData();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 删除行数据
	 */
	@Test
	public void deleteRowData()
	{
		Delete delete = new Delete("row1".getBytes());
		try
		{
			table.delete(delete);
			System.out.println("data delete");
			queryColumnData();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * 删除表
	 */
	@Test
	public void deleteTable()
	{
		try
		{
			admin.disableTable(tn);
			admin.deleteTable(tn);
			if (!admin.tableExists(tn))
			{
				System.out.println(tableName + " is delete !");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	@After
	public void close()
	{
		try
		{
			admin.close();
			conn.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */