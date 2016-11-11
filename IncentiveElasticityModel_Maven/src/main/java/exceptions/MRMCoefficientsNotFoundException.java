/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the coefficients list while calling remove unreasonable
 * coefficients method then it results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMCoefficientsNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 4822223533553890060L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMCoefficientsNotFoundException(String message) {
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
		return "MRMCoefficientsNotFoundException [message=" + getMessage()
				+ "]";
	}

}
