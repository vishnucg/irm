package com.toyota.analytix.mrm.msrp.dataPrep;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.toyota.analytix.common.util.MRMUtil;

public class ModelParameters implements Serializable{
	private static final long serialVersionUID = -7939691527167291741L;
	
	private Map<String, String> propertyMap;
	private static final Logger logger = Logger.getLogger(ModelParameters.class);
	
	private ModelParameters(Map<String, String> propertyMap) {
		this.propertyMap = propertyMap;
	}

	public static ModelParameters getInstance(Map<String, String> propertyMap) {
		ModelParameters m;
		synchronized (ModelParameters.class) {
			m = new ModelParameters(propertyMap);
		}
		return m;
	}

	// TODO Add others and clean out read properties.
	private enum Properties {
		//
		LEXUS_MSRP_COEF_IN_CFTP(null), TOYOTA_MSRP_COEF_IN_CFTP(null),
		//
		MIN_MSRP_ELAS(null), MAX_MSRP_ELAS(null),  DEPENDENT_VAR(null), INCLUSIVE_VAR(null), 
		//
		INDEPEN_VAR_LIST(null), SIGN_VAR_LIST(null), VIF_CUTOFF(null), PVALUE(null),
		// Bayesian
		MSRP_ELAS_RADIUS(null), PRIOR_MSRP_MEAN(null), PRIOR_MSRP_STD_DEV(null), 
		//
		PRIOR_ADJ_FACTOR_NO_BEST_MODEL(null), NO_ENOUGH_DATA_STD_DEV_FACTOR(null), STEPSIZE(null);
		
		private String propertyName; 
		//private Class type; 
		private Properties(String actualName){
			this.propertyName = actualName;
		}
	}

	private String getString(Properties property) {
		String propertyName = getPropertyName(property);
		logger.debug("Property:" + property.toString() +" Fetching String Property for:" + propertyName + " Value: "+ propertyMap.get(propertyName));
		return propertyMap.get(propertyName).trim();
	}

	private String getPropertyName(Properties property) {
		if (property.propertyName != null)
			return property.propertyName;
		String propertyName = property.toString();
		String propertyNameinParameterMap =  MRMUtil.getValue(propertyName);
		logger.debug ("propertyName in props.Properties: " + propertyName + " propertyNameinParameterMap:" + propertyNameinParameterMap);
		return propertyNameinParameterMap;
	}

	private Double getDouble(Properties property) {
		String propertyName = getPropertyName(property);
		logger.debug("Property:" + property.toString() +" Fetching Double Property for:" + propertyName + " Value: "+ propertyMap.get(propertyName));
		try {
			return new Double(propertyMap.get(propertyName).trim());
		} catch (Exception ex) {
			return null;
		}
	}

	private List<String> getList(Properties property) {
		List<String> listOfVariables = null;
		String propertyName = getPropertyName(property);
		logger.debug("Property:" + property.toString() +" Fetching String List  for:" + propertyName + " Value: "+ propertyMap.get(propertyName));
		String varList = propertyMap.get(propertyName).trim();
		if (null != varList) {
			String[] varValuesList = varList.split(",");
			listOfVariables = new ArrayList<>();
			for (String variable : varValuesList) {
				listOfVariables.add(variable.toLowerCase().trim());
			}
		}
		return listOfVariables;
	}

	private List<Integer> getListOfIntegers(Properties property) {
		List<Integer> listOfVariables = null;
		String propertyName = getPropertyName(property);
		logger.debug("Property:" + property.toString() +" Fetching Integer List  for:" + propertyName + " Value: "+ propertyMap.get(propertyName));
		String varList = propertyMap.get(propertyName).trim();
		if (null != varList) {
			String[] varValuesList = varList.split(",");
			listOfVariables = new ArrayList<>();
			for (String variable : varValuesList) {
				listOfVariables.add(Integer.parseInt((variable.toLowerCase().trim())));
			}
		}
		return listOfVariables;
	}

	public Double getMinMSRPElasticity() {
		return getDouble(Properties.MIN_MSRP_ELAS);
	}

	public Double getMaxMSRPElasticity() {
		return getDouble(Properties.MAX_MSRP_ELAS);
	}

	public Double getToyotaCFTPCoef() {
		return getDouble(Properties.TOYOTA_MSRP_COEF_IN_CFTP);
	}

	public Double getLexusCFTPCoef() {
		return getDouble(Properties.LEXUS_MSRP_COEF_IN_CFTP);
	}

	public String getDependentVariable() {
		return getString(Properties.DEPENDENT_VAR);
	}

	public String getInclusiveVariable() {
		return getString(Properties.INCLUSIVE_VAR);
	}

	public List<String> getIndependentVariableList() {
		return getList(Properties.INDEPEN_VAR_LIST);
	}

	public List<Integer> getVariableSignList() {
		return getListOfIntegers(Properties.SIGN_VAR_LIST);
	}

	public double getVIFCutoff() {
		return getDouble(Properties.VIF_CUTOFF);
	}

	public double getPValue() {
		return getDouble(Properties.PVALUE);
	}

	public double getElasticityRadius() {
		return getDouble(Properties.MSRP_ELAS_RADIUS);
	}

	public double getPriorMean() {
		return getDouble(Properties.PRIOR_MSRP_MEAN);
	}

	public double getPriorStandardDeviation() {
		return getDouble(Properties.PRIOR_MSRP_STD_DEV);
	}

	public double getPriorAdjFactor() {
		return getDouble(Properties.PRIOR_ADJ_FACTOR_NO_BEST_MODEL);
	}

	public double getStandardDeviationFactor() {
		return getDouble(Properties.NO_ENOUGH_DATA_STD_DEV_FACTOR);
	}

	public double getStepSize() {
		return getDouble(Properties.STEPSIZE);
	}

	public void resetPriorMean(Double newMean) {
		if (newMean == null)
			return;
		String propertyName = getPropertyName(Properties.PRIOR_MSRP_MEAN);
		logger.debug("Prior mean getting reset to: " + newMean);
		propertyMap.put(propertyName, newMean.toString());
	}
}