package com.toyota.analytix.common.util;

import org.apache.commons.lang3.exception.ExceptionUtils;

public final class Logger {
	
	private Logger(){
		
	}
	
	public static void log (String message){
		Logger.log(message);
	}

	public static void log (String message, Exception e){
		String stackString = ExceptionUtils.getStackTrace(e);
		Logger.log(message+":\n"+stackString);
	}
}
