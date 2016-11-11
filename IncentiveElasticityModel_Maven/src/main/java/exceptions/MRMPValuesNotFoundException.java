/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not found any p-values while calling the remove insignificant variables
 * method then it results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMPValuesNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -310803266441606514L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMPValuesNotFoundException(String message) {
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
		return "MRMPValuesNotFoundException [message=" + getMessage() + "]";
	}

}
