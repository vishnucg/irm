/**
 * 
 */
package com.toyota.analytics.common.regression;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This class contains the members which are holding the linear models
 * information. This class contains the setters and getters for each field to
 * set and get the linear model information. Once we run the regression based on
 * the dependent and independent variables, will get intercept and coefficients
 * and can set to coefficient values map.
 * 
 * @author Naresh
 *
 */
public class LinearModelDTO implements Serializable {

	private static final long serialVersionUID = 7656057765677110161L;

	private String dependantVar;
	private List<String> independentColumnList;
	private List<Double> coefficients;
	private double intercept;
	private Map<String, Double> coefficientValues;

	/**
	 * @return the dependantVar
	 */
	public String getDependantVar() {
		return dependantVar;
	}

	/**
	 * @param dependantVar
	 *            the dependantVar to set
	 */
	public void setDependantVar(String dependantVar) {
		this.dependantVar = dependantVar;
	}

	/**
	 * @return the independentColumnList
	 */
	public List<String> getIndependentColumnList() {
		return independentColumnList;
	}

	/**
	 * @param independentColumnList
	 *            the independentColumnList to set
	 */
	public void setIndependentColumnList(List<String> independentColumnList) {
		this.independentColumnList = independentColumnList;
	}

	/**
	 * @return the coefficients
	 */
	public List<Double> getCoefficients() {
		return coefficients;
	}

	/**
	 * @param coefficients
	 *            the coefficients to set
	 */
	public void setCoefficients(List<Double> coefficients) {
		this.coefficients = coefficients;
	}

	/**
	 * @return the intercept
	 */
	public double getIntercept() {
		return intercept;
	}

	/**
	 * @param intercept
	 *            the intercept to set
	 */
	public void setIntercept(double intercept) {
		this.intercept = intercept;
	}

	/**
	 * @return the coefficientValues
	 */
	public Map<String, Double> getCoefficientValues() {
		return coefficientValues;
	}

	/**
	 * @param coefficientValues
	 *            the coefficientValues to set
	 */
	public void setCoefficientValues(Map<String, Double> coefficientValues) {
		this.coefficientValues = coefficientValues;
	}

}
