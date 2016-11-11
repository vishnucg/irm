/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the independent variables, then it results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMNoIndependentVariablesFoundException extends MRMBaseException {

	private static final long serialVersionUID = -1801560623972746550L;
	private String message = null;;

	/**
	 * 
	 * @param message
	 */
	public MRMNoIndependentVariablesFoundException(String message) {
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
		return "MRMNoIndependentVariablesFoundException [message="
				+ getMessage() + "]";
	}

}
