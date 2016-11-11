/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not get the likelihood values back based on the elasticity values then it
 * results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMLikelihoodListNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 5252334937907383382L;

	private String message = null;

	/**
	 * This will give constructor with one argument.
	 * 
	 * @param message
	 */
	public MRMLikelihoodListNotFoundException(String message) {
		this.message = message;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMLikelihoodListNotFoundException [message=" + message + "]";
	}

}
