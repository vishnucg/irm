/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * do not find data in the input table based on the hive tables query then
 * results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMInputTableDataNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = -7324072439952662722L;

	private String message;

	/**
	 * 
	 * @param message
	 */
	public MRMInputTableDataNotFoundException(String message) {
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
		return "MRMInputTableDataNotFoundException [message=" + message + "]";
	}

}
