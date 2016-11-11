/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the prior probability list based on the gamma distribution with
 * scale and shape.
 * 
 * @author Naresh
 *
 */
public class MRMPriorProbabilityListNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 3587819390646913961L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMPriorProbabilityListNotFoundException(String message) {
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
		return "MRMPriorProbabilityListNotFoundException [message=" + message
				+ "]";
	}

}
