/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * number of rows are less than the number of independent variables list then it
 * results this exception. Here, number of rows should not be less than the
 * independent variables list size otherwise this results exception.
 * 
 * @author Naresh
 *
 */
public class MRMRowsRangeException extends MRMBaseException {

	private static final long serialVersionUID = 101013698279349280L;
	private String message = null;

	/**
	 * This will give constructor with one argument.
	 * 
	 * @param message
	 */
	public MRMRowsRangeException(String message) {
		this.message = message;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMRowsRangeException [message=" + getMessage() + "]";
	}
}
