/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the header information inside the data set, then it results this
 * exception.
 * 
 * @author Naresh
 *
 */
public class MRMHeaderNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -4128040950493883577L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMHeaderNotFoundException(String message) {
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
		return "MRMHeaderNotFoundException [message=" + getMessage() + "]";
	}

}
