package com.achievo.hadoop.zookeeper.curator.mastersel;

import java.io.Serializable;

public class RunningData implements Serializable{


	/**
	 * 
	 */
	private static final long serialVersionUID = -522933808813265389L;
	
    private Long              cid;
    private String            name;
    private boolean           active           = true;
    
    
	public Long getCid() {
		return cid;
	}
	public void setCid(Long cid) {
		this.cid = cid;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
    
    

}
