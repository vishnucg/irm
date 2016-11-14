package com.toyota.analytix.common.util;

public class SimpleTimer {
	private long startTime;
	
	public SimpleTimer (){
		startTime = System.currentTimeMillis();
	}
	
	public long endTimer(){
		return System.currentTimeMillis() - startTime;
	}
	
	public void resetTimer(){
		startTime = System.currentTimeMillis();
	}
	
}
