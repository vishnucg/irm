/**
 * 
 */
package com.toyota.analytix.common.regression;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * * This class contains the fields required for the variable selection vif
 * calculation module. Here we have setters and getters for these fields. If we
 * need any properties from this , we can get using getters, if we want to set,
 * we can set additional properties for this class and can generate the setters
 * and getters.
 * 
 * @author 
 *
 */
public class LinearRegressionModelDTO implements Serializable {

	private static final long serialVersionUID = 5338297322013052441L;

	private Map<String, Double> maxVifValues = null;
	private List<String> excludeColumnsList = null;

	/**
	 * @return the maxVifValues
	 */
	public Map<String, Double> getMaxVifValues() {
		return maxVifValues;
	}

	/**
	 * @param maxVifValues
	 *            the maxVifValues to set
	 */
	public void setMaxVifValues(Map<String, Double> maxVifValues) {
		this.maxVifValues = maxVifValues;
	}

	/**
	 * @return the excludeColumnsList
	 */
	public List<String> getExcludeColumnsList() {
		return excludeColumnsList;
	}

	/**
	 * @param excludeColumnsList
	 *            the excludeColumnsList to set
	 */
	public void setExcludeColumnsList(List<String> excludeColumnsList) {
		this.excludeColumnsList = excludeColumnsList;
	}

}
