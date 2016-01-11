package com.achievo.hadoop.zookeeper.zkclient;

import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

public class SubscribeChildChanges {
	
	private static class ZkChildListener implements IZkChildListener{

		public void handleChildChange(String parentPath,
				List<String> currentChilds) throws Exception {
			// TODO Auto-generated method stub
			
			System.out.println(parentPath);
			System.out.println(currentChilds.toString());
			
		}
		
		
	}

	public static void main(String[] args) throws InterruptedException {
		ZkClient zc = new ZkClient("192.168.1.105:2181",10000,10000,new SerializableSerializer());
		System.out.println("conneted ok!");
		
		// 如果/jike20 节点不存在，在创建这个节点时，也可以监听到事件变化
		zc.subscribeChildChanges("/jike20", new ZkChildListener());
		Thread.sleep(Integer.MAX_VALUE);
		
		
	}
	
}
