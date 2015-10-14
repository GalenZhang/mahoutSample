package com.achievo.hadoop.storm.ordertopology.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.ordertopology.common.FieldNames;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: PairTotalCountBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: PairTotalCountBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 13, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class PairTotalCountBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -6658116785161807799L;

	private OutputCollector collector;

	int totalCount;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		totalCount = 0;
	}

	@Override
	public void execute(Tuple input)
	{
		totalCount++;
		collector.emit(new Values(totalCount));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.TOTAL_COUNT));
	}

}

/*
 * $Log: av-env.bat,v $
 */