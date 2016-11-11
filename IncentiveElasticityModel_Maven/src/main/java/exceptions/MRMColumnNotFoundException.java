/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * required columns not found in the input data set header information then it
 * results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMColumnNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 5349617055620659006L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMColumnNotFoundException(String message) {
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
		return "MRMColumnNotFoundException [message=" + getMessage() + "]";
	}

}
