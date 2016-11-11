/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * required database properties(model, database name and user id) not found then
 * it results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMDbPropertiesNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 6309203946544506325L;

	private String message;

	/**
	 * 
	 * @param message
	 */
	public MRMDbPropertiesNotFoundException(String message) {
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
		return "MRMDbPropertiesNotFoundException [message=" + message + "]";
	}

}
