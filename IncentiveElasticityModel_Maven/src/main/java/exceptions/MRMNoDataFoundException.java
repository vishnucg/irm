/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever we
 * do not find the data inside the input data set, then it results this
 * exception. If input data set does not contains any data , this results
 * exception.
 * 
 * @author Naresh
 *
 */
public class MRMNoDataFoundException extends MRMBaseException {

	private static final long serialVersionUID = -922406418888001615L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMNoDataFoundException(String message) {
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
		return "MRMNoDataException [message=" + getMessage() + "]";
	}

}
