package com.achievo.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFS2TabMapReduce extends Configured implements Tool {
	public static class HDFS2TabMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		ImmutableBytesWritable rowkey = new ImmutableBytesWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
						throws IOException, InterruptedException {
			String[] words = value.toString().split("\t");
			// rk0001 zhangsan 33
			Put put = new Put(Bytes.toBytes(words[0]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(words[1]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(words[2]));

			rowkey.set(Bytes.toBytes(words[0]));

			context.write(rowkey, put);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// create job
		Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());

		// set class
		job.setJarByClass(this.getClass());

		// set path
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// set reduce
		TableMapReduceUtil.initTableReducerJob("user", // set table
				null, job);
		job.setNumReduceTasks(0);

		boolean b = job.waitForCompletion(true);

		if (!b) {
			throw new IOException("error with job!!!");
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// get configuration
		Configuration conf = HBaseConfiguration.create();

		// submit job
		int status = ToolRunner.run(conf, new HDFS2TabMapReduce(), args);

		// exit
		System.exit(status);
	}
}
