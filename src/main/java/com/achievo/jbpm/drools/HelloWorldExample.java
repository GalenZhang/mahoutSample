package com.achievo.jbpm.drools;

import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

/**
 * <pre>
 * 
 *  Accela Automation
 *  File: HelloWorldExample.java
 * 
 *  Accela, Inc.
 *  Copyright (C): 2015
 * 
 *  Description:
 *  TODO
 * 
 *  Notes:
 * 	$Id: HelloWorldExample.java 72642 2009-01-01 20:01:57Z ACHIEVO\galen.zhang $ 
 * 
 *  Revision History
 *  &lt;Date&gt;,			&lt;Who&gt;,			&lt;What&gt;
 *  Oct 14, 2015		galen.zhang		Initial.
 * 
 * </pre>
 */
public class HelloWorldExample
{
	public static void main(String[] args)
	{
		// load up the knowledge base
		KieServices ks = KieServices.Factory.get();
		KieContainer kc = ks.getKieClasspathContainer();
		KieSession ksession = kc.newKieSession("HelloWorldKS");
		
		// go !
		Message message = new Message();
		message.setMessage("Hello World");
		message.setStatus(Message.HELLO);
		ksession.insert(message);
		ksession.fireAllRules();
		ksession.dispose();
	}

	public static class Message
	{
		public static final int HELLO = 0;

		public static final int GOODBYE = 1;

		private String message;

		private int status;

		public String getMessage()
		{
			return message;
		}

		public void setMessage(String message)
		{
			this.message = message;
		}

		public int getStatus()
		{
			return status;
		}

		public void setStatus(int status)
		{
			this.status = status;
		}

	}
}

/*
 * $Log: av-env.bat,v $
 */