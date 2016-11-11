/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the variables list before pass to the calculating the VIF Values
 * for variables, then results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMVariablesNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -7094261529882739159L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMVariablesNotFoundException(String message) {
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
		return "MRMVariablesNotFoundException [message=" + getMessage() + "]";
	}

}
