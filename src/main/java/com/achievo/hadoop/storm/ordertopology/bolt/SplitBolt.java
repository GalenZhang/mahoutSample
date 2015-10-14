package com.achievo.hadoop.storm.ordertopology.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 *  File: SplitBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: SplitBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 13, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class SplitBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = -5008040716708438887L;

	private OutputCollector collector;

	private Map<String, List<String>> orderItems;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
		orderItems = new HashMap<String, List<String>>();
	}

	@Override
	public void execute(Tuple input)
	{
		String id = input.getStringByField(FieldNames.ID);
		String newItem = input.getStringByField(FieldNames.NAME);

		if (!orderItems.containsKey(id))
		{
			List<String> items = new ArrayList<String>();
			items.add(newItem);

			orderItems.put(id, items);
			return;
		}

		List<String> items = orderItems.get(id);
		for (String existItem : items)
		{
			collector.emit(createPair(newItem, existItem));
		}
		items.add(newItem);
	}

	private Values createPair(String item1, String item2)
	{
		if (item1.compareTo(item2) > 0)
		{
			return new Values(item1, item2);
		}
		return new Values(item2, item1);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.ITEM1, FieldNames.ITEM2));
	}

}

/*
 * $Log: av-env.bat,v $
 */