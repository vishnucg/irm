/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs we do not
 * found the properties for the variable selection and bayesian module then
 * results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMPropertiesNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -3662579044670736476L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMPropertiesNotFoundException(String message) {
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
		return "MRMPropertiesNotFoundException [message=" + getMessage() + "]";
	}

}
