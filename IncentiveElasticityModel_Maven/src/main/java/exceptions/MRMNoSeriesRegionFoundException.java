/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * do not find the either series data or regions data in the input file then
 * results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMNoSeriesRegionFoundException extends MRMBaseException {

	private static final long serialVersionUID = -8244217854368248051L;

	private String message;

	/**
	 * 
	 * @param message
	 */
	public MRMNoSeriesRegionFoundException(String message) {
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
		return "MRMNoSeriesRegionFoundException [message=" + message + "]";
	}

}
