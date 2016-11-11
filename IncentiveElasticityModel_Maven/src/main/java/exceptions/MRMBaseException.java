/**
 * 
 */
package exceptions;

/**
 * This exception is base class for all the MRM Exceptions hierarchy. This class
 * has lot of subclasses.
 * 
 * @author Naresh
 *
 */
public class MRMBaseException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6957491739851239778L;

	private String message = null;;

	public MRMBaseException() {
		super();
	}

	/**
	 * 
	 * @param message
	 */
	public MRMBaseException(String message) {
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
		return "MRMBaseException [message=" + getMessage() + "]";
	}

}
