/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.Serializable;

/**
 * @author RLE0326
 *
 */
public class IncentiveDetails implements Serializable {

	private static final long serialVersionUID = -1634063757421745319L;

	private double minIncentivePct;
	private double maxIncentivePct;
	private double avgIncentivePct;

	/**
	 * @return the minIncentivePct
	 */
	public double getMinIncentivePct() {
		return minIncentivePct;
	}

	/**
	 * @param minIncentivePct
	 *            the minIncentivePct to set
	 */
	public void setMinIncentivePct(double minIncentivePct) {
		this.minIncentivePct = minIncentivePct;
	}

	/**
	 * @return the maxIncentivePct
	 */
	public double getMaxIncentivePct() {
		return maxIncentivePct;
	}

	/**
	 * @param maxIncentivePct
	 *            the maxIncentivePct to set
	 */
	public void setMaxIncentivePct(double maxIncentivePct) {
		this.maxIncentivePct = maxIncentivePct;
	}

	/**
	 * @return the avgIncentivePct
	 */
	public double getAvgIncentivePct() {
		return avgIncentivePct;
	}

	/**
	 * @param avgIncentivePct
	 *            the avgIncentivePct to set
	 */
	public void setAvgIncentivePct(double avgIncentivePct) {
		this.avgIncentivePct = avgIncentivePct;
	}
}
