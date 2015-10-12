package com.achievo.hadoop.storm.ordertopology.spout;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.ordertopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: CommandSpout.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: CommandSpout.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 12, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class CommandSpout extends BaseRichSpout
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -6047797690136223383L;

	private static Logger LOG = LoggerFactory.getLogger(CommandSpout.class);

	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
	{
		this.collector = collector;
	}

	@Override
	public void nextTuple()
	{
		try
		{
			Thread.sleep(5000);
		}
		catch (InterruptedException e)
		{
			LOG.info(e.getMessage());
			e.printStackTrace();
		}
		System.err.println("****************---------------------=====================");
		collector.emit(new Values("statistic"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.COMMAND));
	}

}

/*
 * $Log: av-env.bat,v $
 */