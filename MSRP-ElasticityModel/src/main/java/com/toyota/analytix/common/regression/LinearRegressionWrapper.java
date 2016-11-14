package com.toyota.analytix.common.regression;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.FastMath;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;

import scala.collection.JavaConversions;

/**
 * Linear regrssion wrapper class is created to perform all kinds of regressions 
 * such as OLS multiple and simple regression
 */
public class LinearRegressionWrapper {

	protected final Logger logger = Logger.getLogger(getClass());

	//declaring the regression objects for all kinds regression we should use for the model
	private OLSMultipleLinearRegression olsMultipleLinearRegression;
	private SimpleRegression simpleRegression;

	//declaring dependent variable
	private String depVar;

	//declaring list of independent variables
	private List<String> indepVars;

	/**
	 * Constructor which takes the independent and dependent variables
	 * and checks which regression we need to do.If we have more than one independent variable 
	 * it uses OLS Multi regression other wise simple regression
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
	 * For the regression we need to add the data. created a newSampleData method
	 *  to add data to the regression.Create a matrix and take the data from the  
	 * data frame into the array's.Ols regression takes multi dimension array as 
	 * simple regression takes single array
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
			y[j] = variables[j].getDouble(0);
			
			for (int k = 0; k < indepVars.size(); k++) {
				x[j][k] = getDoubleValue(variables[j].get(k+1));
			}
		  //Logger.log(Arrays.toString(x[j]));
		  //Logger.log(y[j]);
		}
		
		logger.debug("inside new sample data");
		
		//
		if (olsMultipleLinearRegression != null) {
			for (double[] doubles : x) {
	               logger.debug(Arrays.toString(doubles));
	           }
	           logger.debug("-----------------------");
	           logger.debug(Arrays.toString(y));

			olsMultipleLinearRegression.newSampleData(y, x);
		} else {
			for (int i = 0; i < y.length; i++) {
				simpleRegression.addData(y[i], x[i][0]);
			}
		}
	}

	private double getDoubleValue(Object o) {
		if (o instanceof Double){
			return (Double)o;
		}else if (o instanceof Integer){
			return ((Integer)o).doubleValue();
		}else if (o instanceof Long){
			return ((Long)o).doubleValue();
		}else if (o instanceof Float){
			return ((Float)o).doubleValue();
		}else if (o instanceof BigDecimal){
			return ((BigDecimal)o).doubleValue();
		}else if (o instanceof String){
			return Double.parseDouble((String)o);
		}else 
			throw new AnalytixRuntimeException("Cannot cast value to Double");
	}


	//Regression takes only double variables. So 
//	private double parseString(String s) {
//	       try {
//	           return Double.parseDouble(s);
//	       } catch (NumberFormatException e) {
//	           StringBuilder number = new StringBuilder();
//	           for (int i = 0; i < s.length(); i++) {
//	               if (s.charAt(i) >= '0' && s.charAt(i) <= '9') {
//	                   number.append(s.charAt(i));
//	               }
//	           }
//	           return Double.parseDouble(number.toString());
//	       }
//	   }

	public double[] estimateResiduals() {
		return olsMultipleLinearRegression.estimateResiduals();
	}
	
	public double getpvalue() {
		return simpleRegression.getSignificance();
	}

	public double getIntercept() {
		return simpleRegression.getIntercept();
	}

	public double getSlope() {
		return simpleRegression.getSlope();
	}

	public double predict(double x) {
		return simpleRegression.predict(x);
	}

	public Map<String, Double> getColumnPValues(
			OLSMultipleLinearRegression regression, String[] columnNames) {
		// mrmDevLogger.info(MessageConstants.METHOD_ENTRANCE_MSG + methodName);
		Map<String, Double> pvalues = new HashMap<>();

		if (null != regression && null != pvalues) {
			// Getting the regression Parameters.
			double[] beta = regression.estimateRegressionParameters();

			// Taking the standard Errors.
			double[] standardErrors = new double[beta.length];
			try {
				standardErrors = regression
						.estimateRegressionParametersStandardErrors();
			} catch (Exception exception) {
				// mrmDevLogger.error(exception.getMessage());
			}

			// Taking residual distribution.
			int residualdf = regression.estimateResiduals().length
					- beta.length;

			// Calculating the p-values.
			for (int i = 0; i < beta.length; i++) {
				// Finding the tstat value.
				double tstat = beta[i] / standardErrors[i];

				// Calculate the pvalue.
				double pvalue = new TDistribution(residualdf)
						.cumulativeProbability(-FastMath.abs(tstat)) * 2;
				pvalues.put(columnNames[i], pvalue);
			}
		}
		return pvalues;
	}

	/**
	 * This method returns the coefficients of the model.
	 * 
	 * @return
	 */
	public List<Double> getCoefficients() {
		OLSMultipleLinearRegression regression = olsMultipleLinearRegression;
		//String methodName = "getCoefficients";
		// mrmDevLogger.info(MessageConstants.METHOD_ENTRANCE_MSG + methodName);
		List<Double> coefficients = new ArrayList<>();

		if (null != regression) {
			// Getting the regression Parameters.
			double[] beta = regression.estimateRegressionParameters();
			for (double value : beta) {
				coefficients.add(value);
			}
		}
		return coefficients;
	}

	/**
	 * This method returns the coefficients of the model.
	 * 
	 * @param y
	 * @param x
	 * @return
	 */
	public Map<String, Double> getColumnCoefficients(
			OLSMultipleLinearRegression regression, String[] columnNames) {
		//String methodName = "getColumnCoefficients";
		// mrmDevLogger.info(MessageConstants.METHOD_ENTRANCE_MSG + methodName);
		Map<String, Double> coefficients = new HashMap<>();

		if (null != regression) {
			// Getting the regression Parameters.
			double[] beta = regression.estimateRegressionParameters();
			if (null != beta && null != columnNames) {
				for (int i = 0; i < beta.length; i++) {
					coefficients.put(columnNames[i], beta[i]);
				}
			}
		}
		return coefficients;
	}

	/**
	 * This will return the regression model based on the matrix.
	 * 
	 * @param y
	 * @param x
	 * @return
	 */
	public OLSMultipleLinearRegression getRegressionModel(double[] y,
			double[][] x) {
		//String methodName = "getRegressionModel";
		// mrmDevLogger.info(MessageConstants.METHOD_ENTRANCE_MSG + methodName);

		OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
		regression.newSampleData(y, x);
		return regression;
	}

	public static JavaRDD<String[]> getHeadersInformation(JavaRDD<String> dataRDD) {
		final String SEPERATE_DELIMETER = ",";

		// Calling map function and return the array of RDD's.
		JavaRDD<String[]> data = dataRDD.map(new Function<String, String[]>() {
			private static final long serialVersionUID = -8120316335738304821L;

			public String[] call(String line) {
				String[] parts = line.split(SEPERATE_DELIMETER);
				return parts;
			}
		});
		return data;
	}
	

}
