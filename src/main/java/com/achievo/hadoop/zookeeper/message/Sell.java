package com.achievo.hadoop.zookeeper.message;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.achievo.hadoop.hdfs.HdfsDAO;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: Sell.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: Sell.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Jul 6, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class Sell
{
	public static final String HDFS = "hdfs://10.50.90.54:9000";

	public static final Pattern DELIMITER = Pattern.compile("[\t,]");

	public static class SellMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private String month = "2013-01";

		private Text k = new Text(month);

		private IntWritable v = new IntWritable();

		private int money = 0;

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException
		{
			System.out.println(value.toString());
			String[] tokens = DELIMITER.split(value.toString());
			if (tokens[3].startsWith(month)) // 1月的数据
			{
				money = Integer.parseInt(tokens[1]) * Integer.parseInt(tokens[2]);// 单价*数量
				v.set(money);
				context.write(k, v);
			}
		}
	}

	public static class SellReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable v = new IntWritable();

		private int money = 0;

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException
		{
			for (IntWritable line : values)
			{
				// System.out.println(key.toString() + "\t" + line);
				money += line.get();
			}
			v.set(money);
			context.write(null, v);
			System.out.println("Output: " + key + " , " + money);
		}
	}

	public static Configuration config()
	{
		Configuration conf = new Configuration();
		conf.addResource("classpath:/hadoop/core-site.xml");
		conf.addResource("classpath:/hadoop/hdfs-site.xml");
		conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}

	public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = config();
		String local_data = path.get("sell");
		String input = path.get("input");
		String output = path.get("output");

		// 初始化sell
		HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(local_data, input);

		Job job = Job.getInstance(conf, "sell");
		job.setJarByClass(Sell.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(SellMapper.class);
		job.setReducerClass(SellReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

	public static Map<String, String> path()
	{
		Map<String, String> path = new HashMap<String, String>();
		path.put("sell", Sell.class.getClassLoader().getResource("").getPath() + "logfile/biz/sell.csv");// 本地的数据文件
		path.put("input", HDFS + "/user/hadoop/biz/sell");// HDFS的目录
		path.put("output", HDFS + "/user/hadoop/biz/sell/output"); // 输出目录
		return path;
	}

	public static void main(String[] args) throws Exception
	{
		run(path());
	}
}

/*
 * $Log: av-env.bat,v $
 */