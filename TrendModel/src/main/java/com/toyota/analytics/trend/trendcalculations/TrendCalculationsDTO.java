/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * This class contains the members which are holding the trending models
 * information. This class contains the setters and getters for each field.
 * 
 * @author Naresh
 *
 */
public class TrendCalculationsDTO implements Serializable {

	private static final long serialVersionUID = -8649122508297311121L;

	private HiveContext hiveContext;
	private Map<String, Integer> headersInformation;
	private int noOfLines;
	private String dependantVar;
	private List<String> independentColumnList;
	private List<Double> coefficients;
	private double intercept;
	private Map<String, Double> coefficientValues;
	private List<Double> forecastMsrpValues;
	private DataFrame trendInput;
	private String series;
	private List<Double> forecastIncentiveValues;
	private List<Double> forecastDealerGrossValues;
	private List<Double> forecastDphValues;
	private List<String> dependentVarsList;
	private String msrpDependantVar;
	private List<String> msrpIndependentColumnList;
	private String incentiveDependantVar;
	private List<String> incentiveIndependentColumnList;
	private String dealergrossDependantVar;
	private List<String> dealergrossIndependentColumnList;

	/**
	 * @return the hiveContext
	 */
	public HiveContext getHiveContext() {
		return hiveContext;
	}

	/**
	 * @param hiveContext
	 *            the hiveContext to set
	 */
	public void setHiveContext(HiveContext hiveContext) {
		this.hiveContext = hiveContext;
	}

	/**
	 * @return the headersInformation
	 */
	public Map<String, Integer> getHeadersInformation() {
		return headersInformation;
	}

	/**
	 * @param headersInformation
	 *            the headersInformation to set
	 */
	public void setHeadersInformation(Map<String, Integer> headersInformation) {
		this.headersInformation = headersInformation;
	}

	/**
	 * @return the noOfLines
	 */
	public int getNoOfLines() {
		return noOfLines;
	}

	/**
	 * @param noOfLines
	 *            the noOfLines to set
	 */
	public void setNoOfLines(int noOfLines) {
		this.noOfLines = noOfLines;
	}

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

	/**
	 * @return the forecastMsrpValues
	 */
	public List<Double> getForecastMsrpValues() {
		return forecastMsrpValues;
	}

	/**
	 * @param forecastMsrpValues
	 *            the forecastMsrpValues to set
	 */
	public void setForecastMsrpValues(List<Double> forecastMsrpValues) {
		this.forecastMsrpValues = forecastMsrpValues;
	}

	/**
	 * @return the trendInput
	 */
	public DataFrame getTrendInput() {
		return trendInput;
	}

	/**
	 * @param trendInput
	 *            the trendInput to set
	 */
	public void setTrendInput(DataFrame trendInput) {
		this.trendInput = trendInput;
	}

	/**
	 * @return the series
	 */
	public String getSeries() {
		return series;
	}

	/**
	 * @param series
	 *            the series to set
	 */
	public void setSeries(String series) {
		this.series = series;
	}

	/**
	 * @return the forecastIncentiveValues
	 */
	public List<Double> getForecastIncentiveValues() {
		return forecastIncentiveValues;
	}

	/**
	 * @param forecastIncentiveValues
	 *            the forecastIncentiveValues to set
	 */
	public void setForecastIncentiveValues(List<Double> forecastIncentiveValues) {
		this.forecastIncentiveValues = forecastIncentiveValues;
	}

	/**
	 * @return the forecastDealerGrossValues
	 */
	public List<Double> getForecastDealerGrossValues() {
		return forecastDealerGrossValues;
	}

	/**
	 * @param forecastDealerGrossValues
	 *            the forecastDealerGrossValues to set
	 */
	public void setForecastDealerGrossValues(
			List<Double> forecastDealerGrossValues) {
		this.forecastDealerGrossValues = forecastDealerGrossValues;
	}

	/**
	 * @return the forecastDphValues
	 */
	public List<Double> getForecastDphValues() {
		return forecastDphValues;
	}

	/**
	 * @param forecastDphValues
	 *            the forecastDphValues to set
	 */
	public void setForecastDphValues(List<Double> forecastDphValues) {
		this.forecastDphValues = forecastDphValues;
	}

	/**
	 * @return the dependentVarsList
	 */
	public List<String> getDependentVarsList() {
		return dependentVarsList;
	}

	/**
	 * @param dependentVarsList
	 *            the dependentVarsList to set
	 */
	public void setDependentVarsList(List<String> dependentVarsList) {
		this.dependentVarsList = dependentVarsList;
	}

	/**
	 * @return the msrpDependantVar
	 */
	public String getMsrpDependantVar() {
		return msrpDependantVar;
	}

	/**
	 * @param msrpDependantVar
	 *            the msrpDependantVar to set
	 */
	public void setMsrpDependantVar(String msrpDependantVar) {
		this.msrpDependantVar = msrpDependantVar;
	}

	/**
	 * @return the msrpIndependentColumnList
	 */
	public List<String> getMsrpIndependentColumnList() {
		return msrpIndependentColumnList;
	}

	/**
	 * @param msrpIndependentColumnList
	 *            the msrpIndependentColumnList to set
	 */
	public void setMsrpIndependentColumnList(
			List<String> msrpIndependentColumnList) {
		this.msrpIndependentColumnList = msrpIndependentColumnList;
	}

	/**
	 * @return the incentiveDependantVar
	 */
	public String getIncentiveDependantVar() {
		return incentiveDependantVar;
	}

	/**
	 * @param incentiveDependantVar
	 *            the incentiveDependantVar to set
	 */
	public void setIncentiveDependantVar(String incentiveDependantVar) {
		this.incentiveDependantVar = incentiveDependantVar;
	}

	/**
	 * @return the incentiveIndependentColumnList
	 */
	public List<String> getIncentiveIndependentColumnList() {
		return incentiveIndependentColumnList;
	}

	/**
	 * @param incentiveIndependentColumnList
	 *            the incentiveIndependentColumnList to set
	 */
	public void setIncentiveIndependentColumnList(
			List<String> incentiveIndependentColumnList) {
		this.incentiveIndependentColumnList = incentiveIndependentColumnList;
	}

	/**
	 * @return the dealergrossDependantVar
	 */
	public String getDealergrossDependantVar() {
		return dealergrossDependantVar;
	}

	/**
	 * @param dealergrossDependantVar
	 *            the dealergrossDependantVar to set
	 */
	public void setDealergrossDependantVar(String dealergrossDependantVar) {
		this.dealergrossDependantVar = dealergrossDependantVar;
	}

	/**
	 * @return the dealergrossIndependentColumnList
	 */
	public List<String> getDealergrossIndependentColumnList() {
		return dealergrossIndependentColumnList;
	}

	/**
	 * @param dealergrossIndependentColumnList
	 *            the dealergrossIndependentColumnList to set
	 */
	public void setDealergrossIndependentColumnList(
			List<String> dealergrossIndependentColumnList) {
		this.dealergrossIndependentColumnList = dealergrossIndependentColumnList;
	}
}
