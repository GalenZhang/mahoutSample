package com.achievo.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: BulkLoadDriver.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://blog.csdn.net/jdplus/article/details/46491737
 * 
 *  Notes:
 * 	$Id: BulkLoadDriver.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 18, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class BulkLoadDriver extends Configured implements Tool
{
	private static final String DATA_SEPERATOR = "\\s+";

	private static final String TABLE_NAME = "temperature";

	private static final String COLUMN_FAMILY_1 = "data"; // 列组1

	private static final String COLUMN_FAMILY_2 = "tempPerHour"; // 列组2

	public static void main(String[] args)
	{
		try
		{
			int response = ToolRunner.run(HBaseConfiguration.create(), new BulkLoadDriver(), args);
			if (response == 0)
			{
				System.out.println("Job is successfully completed...");
			}
			else
			{
				System.out.println("Job failed...");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		String outputPath = args[1];
		Configuration conf = getConf();
		conf.set("data.seperator", DATA_SEPERATOR);
		conf.set("COLUMN_FAMILY_1", COLUMN_FAMILY_1);
		conf.set("COLUMN_FAMILY_2", COLUMN_FAMILY_2);

		Job job = Job.getInstance(conf, "Bulk Loading HBase Table::" + TABLE_NAME);
		job.setJarByClass(BulkLoadDriver.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);
		job.setMapperClass(BulkLoadMapper.class);
		FileInputFormat.addInputPaths(job, args[0]);
		FileSystem fs = FileSystem.get(conf);
		Path output = new Path(outputPath);
		if (fs.exists(output))
		{
			fs.delete(output, true);
		}

		FileOutputFormat.setOutputPath(job, output);
		Connection connection = ConnectionFactory.createConnection();
		TableName tableName = TableName.valueOf(TABLE_NAME);
		HFileOutputFormat2.configureIncrementalLoad(job, connection.getTable(tableName),
			connection.getRegionLocator(tableName));
		job.waitForCompletion(true);

		if (job.isSuccessful())
		{
			HFileLoader.doBulkLoad(outputPath, TABLE_NAME);// 导入数据
			return 0;
		}
		else
		{
			return 1;
		}
	}

	private static class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
	{
		private String dataSeperator;

		private String columnFamily1;

		private String columnFamily2;

		@Override
		protected void setup(Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
				throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			dataSeperator = conf.get("data.seperator");
			columnFamily1 = conf.get("COLUMN_FAMILY_1");
			columnFamily2 = conf.get("COLUMN_FAMILY_2");
		}

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context) throws IOException,
				InterruptedException
		{
			try
			{
				String[] values = value.toString().split(dataSeperator);
				ImmutableBytesWritable rowKey = new ImmutableBytesWritable(values[0].getBytes());
				Put put = new Put(Bytes.toBytes(values[0]));
				put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("month"), Bytes.toBytes(values[1]));
				put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("day"), Bytes.toBytes(values[2]));
				for (int i = 3; i < values.length; i++)
				{
					put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("hour : " + i), Bytes.toBytes(values[i]));
				}
				context.write(rowKey, put);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	private static class HFileLoader
	{
		public static void doBulkLoad(String pathToHFile, String tableName)
		{
			try
			{
				Configuration conf = new Configuration();
				HBaseConfiguration.addHbaseResources(conf);
				LoadIncrementalHFiles loadHFiles = new LoadIncrementalHFiles(conf);
				Connection connection = ConnectionFactory.createConnection();
				TableName table = TableName.valueOf(TABLE_NAME);
				loadHFiles.doBulkLoad(new Path(pathToHFile), connection.getAdmin(), connection.getTable(table),
					connection.getRegionLocator(table));
				System.out.println("Bulk Load Completed..");
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */