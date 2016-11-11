package regression;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import scala.collection.JavaConversions;

/**
 * Linear regression wrapper class is created to perform all kinds of
 * regressions such as OLS multiple and simple regression
 */
public class LinearRegressionWrapper {

	// Initializing the logger.
	static final Logger mrmDevLogger = Logger
			.getLogger(LinearRegressionWrapper.class);

	// declaring the regression objects for all kinds regression we should use
	// for the model
	private OLSMultipleLinearRegression olsMultipleLinearRegression;
	private SimpleRegression simpleRegression;

	// declaring dependent variable
	private String depVar;

	// declaring list of independent variables
	private List<String> indepVars;

	/**
	 * Constructor which takes the independent and dependent variables and
	 * checks which regression we need to do.If we have more than one
	 * independent variable it uses OLS Multi regression other wise simple
	 * regression
	 */
	public LinearRegressionWrapper(String depVar, List<String> indepVars) {
		this.depVar = depVar;
		this.indepVars = indepVars;
		if (indepVars.size() > 1) {
			olsMultipleLinearRegression = new OLSMultipleLinearRegression();
		} else {
			simpleRegression = new SimpleRegression();
		}
	}

	/**
	 * For the regression we need to add the data. created a newSampleData
	 * method to add data to the regression.Create a matrix and take the data
	 * from the data frame into the array's.Ols regression takes multi dimension
	 * array as simple regression takes single array
	 */
	public void newSampleData(DataFrame dataFrame) {
		Row[] variables = dataFrame.select(depVar,
				JavaConversions.asScalaBuffer(indepVars)).collect();
        
		double[] y = new double[variables.length];
		double[][] x = new double[variables.length][indepVars.size()];
		for (int i = 0; i < x.length; i++) {
			Arrays.fill(x[i], 0D);
		}
		for (int j = 0; j < variables.length; j++) {
			try{
			y[j] = variables[j].getDouble(0);
			}
			 catch (ClassCastException e) {
				 y[j] = variables[j].getDecimal(0).doubleValue();
			 }
			for (int k = 0; k < indepVars.size(); k++) {
			
			
				try {
					x[j][k] = variables[j].getDouble(k+1);
				} catch (ClassCastException e) {
					try {
						x[j][k] = variables[j].getInt(k+1);
					} catch (ClassCastException e1) {
						try{
						String stringVal = variables[j].getString(k+1);
						if (null != stringVal && !"".equals(stringVal)){
							x[j][k] = parseString(stringVal);
							
						}
						}catch (ClassCastException e2) {
							 
							x[j][k] = variables[j].getDecimal(k+1).doubleValue();
						}
					}
				}
			
			
			}
		  //mrmDevLogger.warn(Arrays.toString(x[j]));
		  //mrmDevLogger.warn(y[j]);
		}
		
		
		
		//mrmDevLogger.warn("inside new sample data");
		
		//
		if (olsMultipleLinearRegression != null) {
		/*	for (double[] doubles : x) {
	               mrmDevLogger.warn(Arrays.toString(doubles));
	           }
	           mrmDevLogger.warn("-----------------------");
	           mrmDevLogger.warn(Arrays.toString(y));
*/
			
			olsMultipleLinearRegression.newSampleData(y, x);
		} else {
			for (int i = 0; i < y.length; i++) {
				simpleRegression.addData(y[i], x[i][0]);
			}
		}
	}

	/**
	 * 
	 * @param value
	 * @return
	 */
	// Regression takes only double variables. So
	private double parseString(String s) {
		double finalValue = 0.0;
	       try {
	            return Double.parseDouble(s);
	       } catch (NumberFormatException e) {
	           StringBuilder number = new StringBuilder();
	           for (int i = 0; i < s.length(); i++) {
	               if (s.charAt(i) >= '0' && s.charAt(i) <= '9') {
	                   number.append(s.charAt(i));
	               }
	           }
	           try {
					if (!"".equals(number.toString())) {
						finalValue = Double.parseDouble(number.toString());
					}
				} catch (Exception exception) {
				   mrmDevLogger.warn("exception in coverstion of data type");
				}
	           return finalValue;
	       }	}

	/**
	 * This method provides the residuals.
	 * 
	 * @return residuals
	 */
	public double[] estimateResiduals() {
		double[] residuals = null;
		if (null == olsMultipleLinearRegression) {

		} else {
			residuals = olsMultipleLinearRegression.estimateResiduals();
		}
		return residuals;
	}

	/**
	 * 
	 * @return pvalue
	 */
	public double getpvalue() {
		double pvalue = 0.0;
		if (null == simpleRegression) {

		} else {
			pvalue = simpleRegression.getSignificance();
		}
		return pvalue;
	}

	/**
	 * 
	 * @return intercept
	 */
	public double getIntercept() {
		double intercept = 0.0;
		if (null == simpleRegression) {

		} else {
			intercept = simpleRegression.getIntercept();
		}
		return intercept;
	}

	/**
	 * This method returns the slope.
	 * 
	 * @return slope
	 */
	public double getSlope() {
		double slope = 0.0;
		if (null == simpleRegression) {

		} else {
			slope = simpleRegression.getSlope();
		}
		return slope;
	}

	/**
	 * This method predicts the value.
	 * 
	 * @param x
	 * @return predict
	 */
	public double predict(double x) {
		double predict = 0.0;
		if (null == simpleRegression) {

		} else {
			predict = simpleRegression.predict(x);
		}
		return predict;
	}

}
