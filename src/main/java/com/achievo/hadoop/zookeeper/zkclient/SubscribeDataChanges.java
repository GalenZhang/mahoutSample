package com.achievo.hadoop.zookeeper.zkclient;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

public class SubscribeDataChanges {
	
	private static class ZkDataListener implements IZkDataListener{

		public void handleDataChange(String dataPath, Object data)
				throws Exception {
			// TODO Auto-generated method stub
			System.out.println(dataPath+":"+data.toString());
		}

		public void handleDataDeleted(String dataPath) throws Exception {
			// TODO Auto-generated method stub
			System.out.println(dataPath);
			
		}

		
		
		
	}

	public static void main(String[] args) throws InterruptedException {
		// SerializableSerializer 序列化是基于java 对象的，如果在zkCli 中直接修改数据，则与反序列化不匹配，不能获得到数据变化的监听
		// 所以这里的序列化修改成：BytesPushThroughSerializer 基于字节数组
		ZkClient zc = new ZkClient("192.168.1.105:2181",10000,10000,new BytesPushThroughSerializer());
		System.out.println("conneted ok!");
		
		zc.subscribeDataChanges("/jike20", new ZkDataListener());
		Thread.sleep(Integer.MAX_VALUE);
		
		
	}
	
}
