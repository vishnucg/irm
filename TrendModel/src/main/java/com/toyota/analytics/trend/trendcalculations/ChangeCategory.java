/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.Serializable;

/**
 * This class contains setters and getters for the avgMsrpChgPct, majorChange,
 * MinorChange and regular change. This holds the change category values like
 * major, minor and regular values.
 * 
 * @author Naresh
 *
 */
public class ChangeCategory implements Serializable {

	private static final long serialVersionUID = 4068985735203128985L;

	private double avgMsrpChgPct;
	private String majorChange;
	private String minorChange;
	private String regularChange;

	/**
	 * @return the avgMsrpChgPct
	 */
	public double getAvgMsrpChgPct() {
		return avgMsrpChgPct;
	}

	/**
	 * @param avgMsrpChgPct
	 *            the avgMsrpChgPct to set
	 */
	public void setAvgMsrpChgPct(double avgMsrpChgPct) {
		this.avgMsrpChgPct = avgMsrpChgPct;
	}

	/**
	 * @return the majorChange
	 */
	public String getMajorChange() {
		return majorChange;
	}

	/**
	 * @param majorChange
	 *            the majorChange to set
	 */
	public void setMajorChange(String majorChange) {
		this.majorChange = majorChange;
	}

	/**
	 * @return the minorChange
	 */
	public String getMinorChange() {
		return minorChange;
	}

	/**
	 * @param minorChange
	 *            the minorChange to set
	 */
	public void setMinorChange(String minorChange) {
		this.minorChange = minorChange;
	}

	/**
	 * @return the regularChange
	 */
	public String getRegularChange() {
		return regularChange;
	}

	/**
	 * @param regularChange
	 *            the regularChange to set
	 */
	public void setRegularChange(String regularChange) {
		this.regularChange = regularChange;
	}

}
