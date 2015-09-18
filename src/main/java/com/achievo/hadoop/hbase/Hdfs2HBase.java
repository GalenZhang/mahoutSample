package com.achievo.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: Hdfs2HBase.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  http://songlee24.github.io/2015/08/13/hdfs-import-to-hbase/
 * 
 *  Notes:
 * 	$Id: Hdfs2HBase.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Sep 17, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class Hdfs2HBase
{
	// create 'mytable','cf'
	// list
	// scan 'mytable'

	// data.txt
	// r1:cf:c1:value1
	// r2:cf:c2:value2
	// r3:cf:c3:value3

	// ./bin/hadoop fs -put /home/hadoop/temp/data.txt /tmp
	// ./bin/hadoop fs -cat /tmp/data.txt
	// ./bin/hadoop jar hdfs2hbase.jar com.achievo.hadoop.hbase.Hdfs2HBase /tmp/data.txt mytable

	public static class Hdfs2HBaseMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException
		{
			String lineStr = value.toString();
			int index = lineStr.indexOf(":");
			String rowKey = lineStr.substring(0, index);
			String left = lineStr.substring(index + 1);
			context.write(new Text(rowKey), new Text(left));
		}
	}

	public static class Hdfs2HBaseReducer extends Reducer<Text, Text, ImmutableBytesWritable, Put>
	{
		@Override
		protected void reduce(Text rowKey, Iterable<Text> value,
				Reducer<Text, Text, ImmutableBytesWritable, Put>.Context context) throws IOException,
				InterruptedException
		{

			String k = rowKey.toString();
			for (Text val : value)
			{
				Put put = new Put(k.getBytes());
				String[] strs = val.toString().split(":");
				String family = strs[0];
				String qualifier = strs[1];
				String v = strs[2];
				put.addColumn(family.getBytes(), qualifier.getBytes(), v.getBytes());
				context.write(new ImmutableBytesWritable(k.getBytes()), put);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: wordcount <infile> <table>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "hdfs2hbase");
		job.setJarByClass(Hdfs2HBase.class);
		job.setMapperClass(Hdfs2HBaseMapper.class);
		job.setReducerClass(Hdfs2HBaseReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Put.class);

		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, otherArgs[1]);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

/*
 * $Log: av-env.bat,v $
 */