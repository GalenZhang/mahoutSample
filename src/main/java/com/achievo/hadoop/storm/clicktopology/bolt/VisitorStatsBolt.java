package com.achievo.hadoop.storm.clicktopology.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.clicktopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: VisitorStatsBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: VisitorStatsBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class VisitorStatsBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -4816623216668698440L;

	private OutputCollector collector;

	private int total = 0;

	private int uniqueCount = 0;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input)
	{
		boolean unique = Boolean.parseBoolean(input.getStringByField(FieldNames.UNIQUE));
		if (unique)
		{
			uniqueCount++;
		}
		collector.emit(new Values(total, uniqueCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.TOTAL_COUNT, FieldNames.TOTAL_UNIQUE));
	}

}

/*
 * $Log: av-env.bat,v $
 */