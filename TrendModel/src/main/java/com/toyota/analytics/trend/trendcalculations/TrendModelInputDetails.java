/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;

/**
 * This class contains setters and getters which will set and get the values for
 * the required details like maxModelYear, previousMsrpValue, maxBusinessMonth,
 * dphValue and seriesFrame.
 * 
 * @author Naresh
 *
 */
public class TrendModelInputDetails implements Serializable {

	private static final long serialVersionUID = -5944437799748215762L;

	private int maxModelYear;
	private double previousMsrpValue;
	private String maxBusinessMonth;
	private double dphValue;
	private DataFrame seriesFrame;
	private int minModelYear;

	/**
	 * @return the maxModelYear
	 */
	public int getMaxModelYear() {
		return maxModelYear;
	}

	/**
	 * @param maxModelYear
	 *            the maxModelYear to set
	 */
	public void setMaxModelYear(int maxModelYear) {
		this.maxModelYear = maxModelYear;
	}

	/**
	 * @return the previousMsrpValue
	 */
	public double getPreviousMsrpValue() {
		return previousMsrpValue;
	}

	/**
	 * @param previousMsrpValue
	 *            the previousMsrpValue to set
	 */
	public void setPreviousMsrpValue(double previousMsrpValue) {
		this.previousMsrpValue = previousMsrpValue;
	}

	/**
	 * @return the maxBusinessMonth
	 */
	public String getMaxBusinessMonth() {
		return maxBusinessMonth;
	}

	/**
	 * @param maxBusinessMonth
	 *            the maxBusinessMonth to set
	 */
	public void setMaxBusinessMonth(String maxBusinessMonth) {
		this.maxBusinessMonth = maxBusinessMonth;
	}

	/**
	 * @return the dphValue
	 */
	public double getDphValue() {
		return dphValue;
	}

	/**
	 * @param dphValue
	 *            the dphValue to set
	 */
	public void setDphValue(double dphValue) {
		this.dphValue = dphValue;
	}

	/**
	 * @return the seriesFrame
	 */
	public DataFrame getSeriesFrame() {
		return seriesFrame;
	}

	/**
	 * @param seriesFrame
	 *            the seriesFrame to set
	 */
	public void setSeriesFrame(DataFrame seriesFrame) {
		this.seriesFrame = seriesFrame;
	}

	/**
	 * @return the minModelYear
	 */
	public int getMinModelYear() {
		return minModelYear;
	}

	/**
	 * @param minModelYear
	 *            the minModelYear to set
	 */
	public void setMinModelYear(int minModelYear) {
		this.minModelYear = minModelYear;
	}

}
