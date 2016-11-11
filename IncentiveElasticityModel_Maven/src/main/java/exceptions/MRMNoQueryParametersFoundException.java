/**
 * 
 */
package exceptions;

/**
 * This exception is subclass of the MRMBaseException. This occurs whenever the
 * do not find query parameters from the properties file then results this
 * exception.
 * 
 * @author Naresh
 *
 */
public class MRMNoQueryParametersFoundException extends MRMBaseException {

	private static final long serialVersionUID = -7523672626543420040L;

	private String message;

	/**
	 * 
	 * @param message
	 */
	public MRMNoQueryParametersFoundException(String message) {
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
		return "MRMNoQueryParametersFoundException [message=" + message + "]";
	}

}
