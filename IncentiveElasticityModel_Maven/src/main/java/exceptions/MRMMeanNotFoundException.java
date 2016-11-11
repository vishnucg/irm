/**
 * 
 */
package exceptions;

/**
 * This exception raises whenever the finding the mean values of this
 * columns(incentive_comp_ratio
 * ,comp_incentive,cftp,pred_value,msrp_comp_ratio,residual) are failed. If we
 * do not find these columns inside input data set or not find any mean for the
 * particular column results this exception.
 * 
 * @author Naresh
 *
 */
public class MRMMeanNotFoundException extends MRMBaseException {

	private static final long serialVersionUID = 2609927036143269963L;

	private String message = null;

	/**
	 * 
	 * @param message
	 */
	public MRMMeanNotFoundException(String message) {
		this.message = message;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return "MRMMeanNotFoundException [message=" + getMessage() + "]";
	}

}
