/**
 * 
 */
package com.toyota.analytix.mrm.msrp.bayesian;

import java.io.Serializable;

/**
 * This class contains the fields required for the bayesian module while reading
 * the data from the input data set these fields will be used. Here we have
 * setters and getters for these fields. If we need any properties from this ,
 * we can get using getters, if we want to set, we can set the properties using
 * setters.
 * 
 * @author 
 *
 */
public class BayesianPropertiesDTO implements Serializable {

	private static final long serialVersionUID = 1815296877408238335L;

	private double centeredResudual;
	private double centeredAvgCompRatio;
	private double predcenteredResidual;
	private double deviationVal;
	private double predValue;

	/**
	 * @return the centeredResudual
	 */
	public double getCenteredResudual() {
		return centeredResudual;
	}

	/**
	 * @param centeredResudual
	 *            the centeredResudual to set
	 */
	public void setCenteredResudual(double centeredResudual) {
		this.centeredResudual = centeredResudual;
	}

	/**
	 * @return the centeredAvgCompRatio
	 */
	public double getCenteredAvgCompRatio() {
		return centeredAvgCompRatio;
	}

	/**
	 * @param centeredAvgCompRatio
	 *            the centeredAvgCompRatio to set
	 */
	public void setCenteredAvgCompRatio(double centeredAvgCompRatio) {
		this.centeredAvgCompRatio = centeredAvgCompRatio;
	}

	/**
	 * @return the predcenteredResidual
	 */
	public double getPredcenteredResidual() {
		return predcenteredResidual;
	}

	/**
	 * @param predcenteredResidual
	 *            the predcenteredResidual to set
	 */
	public void setPredcenteredResidual(double predcenteredResidual) {
		this.predcenteredResidual = predcenteredResidual;
	}

	/**
	 * @return the deviationVal
	 */
	public double getDeviationVal() {
		return deviationVal;
	}

	/**
	 * @param deviationVal
	 *            the deviationVal to set
	 */
	public void setDeviationVal(double deviationVal) {
		this.deviationVal = deviationVal;
	}

	/**
	 * @return the predValue
	 */
	public double getPredValue() {
		return predValue;
	}

	/**
	 * @param predValue
	 *            the predValue to set
	 */
	public void setPredValue(double predValue) {
		this.predValue = predValue;
	}

}
