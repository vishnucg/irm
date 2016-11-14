/**
 * 
 */
package com.toyota.analytix.common.exceptions;

/**
 * Unchecked exception for all analytix exceptions
 * @author 
 *
 */
public class AnalytixRuntimeException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6957491739851239778L;

	private String message = null;;

	public AnalytixRuntimeException() {
		super();
	}

	public AnalytixRuntimeException(Exception e) {
		super(e);
	}
	/**
	 * 
	 * @param message
	 */
	public AnalytixRuntimeException(String message) {
		this.message = message;
	}

	/**
	 * @return the message
	 */
	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMBaseException [message=" + getMessage() + "]";
	}

}
