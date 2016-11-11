/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the posterior probability list based on the prior probability
 * list and likelihood values.
 * 
 * @author Naresh
 *
 */
public class MRMPostProbabilityListNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -5664678758521879557L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMPostProbabilityListNotFoundException(String message) {
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
		return "MRMPostProbabilityListNotFoundException [message=" + message
				+ "]";
	}

}
