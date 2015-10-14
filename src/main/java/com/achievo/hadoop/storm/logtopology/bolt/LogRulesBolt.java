package com.achievo.hadoop.storm.logtopology.bolt;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.achievo.hadoop.storm.logtopology.common.FieldNames;
import com.achievo.hadoop.storm.logtopology.model.LogEntry;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: LogRulesBolt.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: LogRulesBolt.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class LogRulesBolt extends BaseRichBolt
{

	/**
	 * Comment for &lt;code&gt;serialVersionUID&lt;/code&gt;
	 */
	private static final long serialVersionUID = 8073985529275029677L;

	public static Logger LOG = LoggerFactory.getLogger(LogRulesBolt.class);

	private KieSession ksession;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
	{
		this.collector = collector;

		// load up the knowledge base
		KieServices ks = KieServices.Factory.get();
		KieContainer kc = ks.getKieClasspathContainer();
		ksession = kc.newKieSession("LogRulesKS");
	}

	@Override
	public void execute(Tuple input)
	{
		LogEntry entry = (LogEntry) input.getValueByField(FieldNames.LOG_ENTRY);
		if (entry == null)
		{
			LOG.error("Received null or incorrect value from tuple.");
		}

		ksession.insert(entry);
		ksession.fireAllRules();
		if (!entry.isFilter())
		{
			LOG.debug("Emitting from Rules Bolt");
			collector.emit(new Values(entry));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer)
	{
		declarer.declare(new Fields(FieldNames.LOG_ENTRY));
	}

	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		KieServices ks = KieServices.Factory.get();
		KieContainer kc = ks.getKieClasspathContainer();
		KieSession ksession = kc.newKieSession("LogRulesKS");

		JSONObject json = new JSONObject();
		json.put("@source", "acclea");
		json.put("@timestamp", DateFormat.getDateInstance().format(new Date()));
		json.put("@source_host", "galen-zhang.achievo.com");
		json.put("@source_path", "V360");
		json.put("@message", "This is test data.");
		json.put("@type", "test");

		List<String> tags = new ArrayList<String>();
		tags.add("drools");
		JSONArray temp = new JSONArray();
		temp.addAll(tags);
		json.put("@tags", temp);

		JSONObject fieldTemp = new JSONObject();
		Map<String, String> fields = new HashMap<String, String>();
		fields.put("drools", "test");
		fieldTemp.putAll(fields);
		json.put("@fields", fieldTemp);

		LogEntry entry = new LogEntry(json);
		ksession.insert(entry);
		ksession.fireAllRules();
		ksession.dispose();

		if (entry.isFilter())
		{
			System.out.println("Test data filter value is true.");
		}
		else
		{
			System.out.println("Test data filter value is false.");
		}
	}
}

/*
 * $Log: av-env.bat,v $
 */