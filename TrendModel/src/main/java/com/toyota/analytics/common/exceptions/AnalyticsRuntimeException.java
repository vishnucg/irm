/**
 * 
 */
package com.toyota.analytics.common.exceptions;

/**
 * This exception is subclass of the RuntimeException. This occurs whenever we
 * face the run time exception while doing analytics then it results this
 * exception.
 * 
 * @author Naresh
 *
 */
public class AnalyticsRuntimeException extends Exception {

	private static final long serialVersionUID = 1405952750343158560L;

	private String message;

	public AnalyticsRuntimeException() {
		super();
	}

	/**
	 * 
	 * @param message
	 */
	public AnalyticsRuntimeException(String message) {
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
		return "AnalyticsRuntimeException [message=" + getMessage() + "]";
	}

}
