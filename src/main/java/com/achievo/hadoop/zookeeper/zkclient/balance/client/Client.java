package com.achievo.hadoop.zookeeper.zkclient.balance.client;

public interface Client {

	public void connect() throws Exception;
	public void disConnect() throws Exception;
	
}
