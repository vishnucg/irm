/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the VIF values for the variables list, then it results this
 * exception.
 * 
 * @author Naresh
 *
 */
public class MRMVifNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 1290347401579546546L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMVifNotFoundException(String message) {
		this.message = message;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMVifNotFoundException [message=" + getMessage() + "]";
	}

}
