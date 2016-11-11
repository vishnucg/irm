/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not get the model back based on the data then it results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMLinearModelNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -2564845442659386020L;

	private String message = null;

	/**
	 * This will give constructor with one argument.
	 * 
	 * @param message
	 */
	public MRMLinearModelNotFoundException(String message) {
		this.message = message;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMLinearModelNotFoundException [message=" + getMessage() + "]";
	}

}
