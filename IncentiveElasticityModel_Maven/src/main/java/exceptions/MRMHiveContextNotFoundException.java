/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * do not find the hive context then it results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMHiveContextNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -7593662542031157715L;

	private String message;

	/**
	 * 
	 * @param message
	 */
	public MRMHiveContextNotFoundException(String message) {
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
		return "MRMHiveContextNotFoundException [message=" + message + "]";
	}

}
