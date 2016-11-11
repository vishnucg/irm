/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the data in the data frame , this results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMNoFrameDataFoundException extends MRMBaseException {

	private static final long serialVersionUID = -4786052228544791064L;

	private String message;

	/**
	 * 
	 * @param message
	 */
	public MRMNoFrameDataFoundException(String message) {
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
		return "MRMNoFrameDataFoundException [message=" + message + "]";
	}

}
