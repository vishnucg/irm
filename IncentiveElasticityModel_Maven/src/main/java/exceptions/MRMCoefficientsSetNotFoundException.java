/**
 * 
 */
package exceptions;

/**
 * This exception raises whenever face the problem trying to calculate the
 * coefficients values based on the mean values and elasticity list.
 * 
 * @author Naresh
 *
 */
public class MRMCoefficientsSetNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 3863027337040139491L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMCoefficientsSetNotFoundException(String message) {
		this.message = message;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMCoefficientsSetNotFoundException [message=" + getMessage()
				+ "]";
	}

}
