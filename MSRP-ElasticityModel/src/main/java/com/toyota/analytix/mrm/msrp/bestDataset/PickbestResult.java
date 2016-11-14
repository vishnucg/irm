package com.toyota.analytix.mrm.msrp.bestDataset;

import java.io.Serializable;

import org.apache.spark.sql.DataFrame;

public class PickbestResult implements Serializable {

	private static final long serialVersionUID = 1L;

	private String formula;
	private double pValue;
	private double elasticity;
	private DataFrame resultDataFrame;

	public PickbestResult(DataFrame dataFrame, double pValue, double elasticity, String formula) {
		this.resultDataFrame = dataFrame;
		this.pValue = pValue;
		this.elasticity = elasticity;
		this.formula = formula;
	}

	public String getFormula() {
		return formula;
	}

	public double getpValue() {
		return pValue;
	}

	public double getElasticity() {
		return elasticity;
	}

	public DataFrame getResultDataFrame() {
		return resultDataFrame;
	}

}
