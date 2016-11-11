/**
 * 
 */
package exceptions;

/**
 * This exception raises whenever the we got the problem with the finding the
 * elasticity values with minimum and maximum values.
 * 
 * @author Naresh
 *
 */
public class MRMElasticitiesNotFoundException extends MRMBaseException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4235528236990213040L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMElasticitiesNotFoundException(String message) {
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
		return "MRMElasticitiesNotFoundException [message=" + getMessage()
				+ "]";
	}

}
