/**
 * 
 */
package com.toyota.analytix.common.variableSelection;

import java.io.Serializable;
import java.util.List;

/**
 * This class contains the fields required for the variable selection vif
 * calculation. Here we have setters and getters for these fields. If we need
 * any properties from this , we can get using getters, if we want to set, we
 * can set additional properties for this class and can generate the setters and
 * getters.
 * 
 * @author 
 *
 */
public class VIFPropertiesDTO implements Serializable {

	private static final long serialVersionUID = 5102299913884176754L;

	private List<String> varList;
	private double vifCutoff;

	/**
	 * @return the varList
	 */
	public List<String> getVarList() {
		return varList;
	}

	/**
	 * @param varList
	 *            the varList to set
	 */
	public void setVarList(List<String> varList) {
		this.varList = varList;
	}

	/**
	 * @return the vifCutoff
	 */
	public double getVifCutoff() {
		return vifCutoff;
	}

	/**
	 * @param vifCutoff
	 *            the vifCutoff to set
	 */
	public void setVifCutoff(double vifCutoff) {
		this.vifCutoff = vifCutoff;
	}

}
