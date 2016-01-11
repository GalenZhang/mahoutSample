package com.achievo.hadoop.zookeeper.demo;

import java.io.IOException;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;


public class CreateNodeASync implements Watcher { 

	private static ZooKeeper zookeeper;
	public static void main(String[] args) throws IOException, InterruptedException {
		zookeeper = new ZooKeeper("192.168.1.105:2181",5000,new CreateNodeASync());
		System.out.println(zookeeper.getState());
		
		Thread.sleep(Integer.MAX_VALUE);
	}
	
	private void doSomething(){
	
		zookeeper.create("/node_5", "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,new IStringCallback(),"创建");
			
		
	}
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("收到事件："+event);
		if (event.getState()==KeeperState.SyncConnected){
			if (event.getType()==EventType.None && null==event.getPath()){
				doSomething();
			}
		}
	}
	
	static class IStringCallback implements AsyncCallback.StringCallback{
		
		/*
		 * rc：返回结果码，0 表示正常返回
		 * path：创建的路径
		 * ctx：异常调用的上下文，就是在create 方法中，传入的最后一个参数："创建"
		 * name：实际创建的路径
		 * @see org.apache.zookeeper.AsyncCallback.StringCallback#processResult(int, java.lang.String, java.lang.Object, java.lang.String)
		 */
		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			// TODO Auto-generated method stub
			StringBuilder sb = new StringBuilder();
			sb.append("rc="+rc).append("\n");
			sb.append("path="+path).append("\n");
			sb.append("ctx="+ctx).append("\n");
			sb.append("name="+name);
			System.out.println(sb.toString());
			
		}
		
		
	}
	
}
