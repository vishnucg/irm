/**
 * 
 */
package com.toyota.analytics.common.exceptions;

/**
 * This exception is subclass of the AnalyticsRuntimeException. This occurs
 * whenever we have done the minimum data validation on the history data frame ,
 * then if we find the the rules are not satisfied, this results this exception.
 * 
 * @author Naresh
 *
 */
public class AnalyticsDataValidationException extends AnalyticsRuntimeException {

	private static final long serialVersionUID = -7773624038417212103L;

	private String message;

	public AnalyticsDataValidationException() {
		super();
	}

	/**
	 * 
	 * @param message
	 */
	public AnalyticsDataValidationException(String message) {
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
		return "AnalyticsDataValidationException [message=" + getMessage()
				+ "]";
	}

}
