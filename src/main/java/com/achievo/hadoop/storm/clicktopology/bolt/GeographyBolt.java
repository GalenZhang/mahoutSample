package com.achievo.hadoop.storm.clicktopology.bolt;

import java.util.Map;

import org.json.simple.JSONObject;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.clicktopology.common.FieldNames;
import com.achievo.hadoop.storm.clicktopology.common.IPResolver;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: GeographyBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: GeographyBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 10, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class GeographyBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 1627671025080347050L;

	private IPResolver resolver;

	private OutputCollector collector;

	public GeographyBolt(IPResolver resolver)
	{
		this.resolver = resolver;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input)
	{
		String ip = input.getStringByField(FieldNames.IP);
		JSONObject json = resolver.resolveIP(ip);

		String city = (String) json.get(FieldNames.CITY);
		String country = (String) json.get(FieldNames.COUNTRY_NAME);
		collector.emit(new Values(country, city));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.COUNTRY, FieldNames.CITY));
	}

}

/*
 * $Log: av-env.bat,v $
 */