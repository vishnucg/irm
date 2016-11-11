/**
 * 
 */
package regression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.FastMath;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import scala.collection.JavaConversions;
import util.MRMIndexes;
import exceptions.MRMLinearModelNotFoundException;

/**
 * @author Naresh
 *
 */
public class LinearModel {

	static final Logger mrmDevLogger = Logger.getLogger(LinearModel.class);

	private static final String EXC_MSG = "MODEL_EXCE";
	private OLSMultipleLinearRegression olsMultipleLinearRegression = null;
	private SimpleRegression simpleRegression = null;

	/**
	 * This constructor will takes the parameters x and y, based on these
	 * parameters will provide the simple regression model and gives you the
	 * calculated values by providing the getter methods inside this.Here, x is
	 * dependent variable, y is independent variable.
	 * 
	 * @param x
	 * @param y
	 */
	public LinearModel(double[] x, double[] y) {
		if (null == simpleRegression) {
			mrmDevLogger.info("Simple Linear Regression is called.");
			simpleRegression = new SimpleRegression();
			if (null != x && x.length > 0 && null != y && y.length > 0) {
				for (int i = 0; i < x.length; i++) {
					simpleRegression.addData(x[i], y[i]);
				}
			}
		}
	}

	/**
	 * This constructor will takes the parameters x and y, based on these
	 * parameters will provide the simple regression model and gives you the
	 * calculated values by providing the getter methods inside this. Here, x is
	 * independent variable,y is dependent variable and intercept can set it
	 * either true or false.
	 * 
	 * @param x
	 * @param y
	 * @param intercept
	 */
	public LinearModel(double[] x, double[] y, boolean intercept) {
		if (null == simpleRegression) {
			mrmDevLogger.info("Simple Linear Regression is called.");
			simpleRegression = new SimpleRegression(intercept);
			if (null != x && x.length > 0 && null != y && y.length > 0) {
				for (int i = 0; i < x.length; i++) {
					simpleRegression.addData(x[i], y[i]);
				}
			}
		}
	}

	/**
	 * This constructor will takes the parameters y and x based on these
	 * parameters will provide the OLS multiple linear regression model and
	 * gives you the calculated values by providing the getter methods inside
	 * this. Here is x is independent variables data and y is dependent
	 * variables data.
	 * 
	 * @param y
	 * @param x
	 */
	public LinearModel(double[] y, double[][] x) {
		if (null != x && x.length > 0 && null != y && y.length > 0) {
			olsMultipleLinearRegression = new OLSMultipleLinearRegression();
			olsMultipleLinearRegression.newSampleData(y, x);
		}
	}

	/**
	 * This constructor will takes the parameters y and x based on these
	 * parameters will provide the OLS multiple linear regression model and
	 * gives you the calculated values by providing the getter methods inside
	 * this. Here is x is independent variables data and y is dependent
	 * variables data and intercept can set for the multiple linear regression
	 * model.
	 * 
	 * @param y
	 * @param x
	 * @param intercept
	 */
	public LinearModel(double[] y, double[][] x, boolean intercept) {
		if (null != x && x.length > 0 && null != y && y.length > 0) {
			olsMultipleLinearRegression = new OLSMultipleLinearRegression();
			try {
				setIntercept(intercept);
				olsMultipleLinearRegression.newSampleData(y, x);
			} catch (MRMLinearModelNotFoundException exception) {
				mrmDevLogger.error(exception.getMessage());
			}

		}
	}

	/**
	 * This constructor takes the data frame, dependent variable and independent
	 * variables. This will create model based on the input list size, if the
	 * size is more than 1 , this will create OLS Linear regression model based
	 * on the data if not this will create the simple linear regression model.
	 * Here, Need to pass dependent variable value and independent variables
	 * list for this method.
	 * 
	 * @param dataFrame
	 * @param depVar
	 * @param indepVars
	 */
	public LinearModel(DataFrame dataFrame, String depVar,
			List<String> indepVars) {
		if (null != dataFrame && null != depVar && !indepVars.isEmpty()) {
			Row[] variables = dataFrame.select(depVar,
					JavaConversions.asScalaBuffer(indepVars)).collect();
			if (null != variables && variables.length > 0) {
				double[] y = new double[variables.length];
				double[][] x = new double[variables.length][indepVars.size()];
				for (int i = 0; i < x.length; i++) {
					Arrays.fill(x[i], 0D);
				}
				for (int j = 0; j < variables.length; j++) {
					y[j] = variables[j].getDouble(0);
					for (int k = 0; k < indepVars.size(); k++) {
						try {
							x[j][k] = variables[j].getDouble(k + 1);
						} catch (ClassCastException e) {
							try {
								x[j][k] = variables[j].getInt(k + 1);
							} catch (ClassCastException e1) {
								x[j][k] = parseString(variables[j]
										.getString(k + 1));
							}
						}
					}
				}
				checkModelType(depVar, indepVars, y, x);
			}
		}
	}

	/**
	 * This method checks the model type based on the size of the list.
	 * 
	 * @param depVar
	 * @param indepVars
	 * @param y
	 * @param x
	 */
	private void checkModelType(String depVar, List<String> indepVars,
			double[] y, double[][] x) {
		generateModelInstance(depVar, indepVars);
		if (null != olsMultipleLinearRegression) {
			LinearModel linearModel = new LinearModel(y, x);
			mrmDevLogger.info(linearModel);
		} else {
			simpleRegression = new SimpleRegression();
			for (int i = 0; i < y.length; i++) {
				simpleRegression.addData(y[i], x[i][0]);
			}
		}
	}

	/**
	 * This method will create the model based on the input list size.
	 * 
	 * @param depVar
	 * @param indepVars
	 */
	private void generateModelInstance(String depVar, List<String> indepVars) {
		if (null != depVar && null != indepVars && !indepVars.isEmpty()) {
			if (indepVars.size() > 1) {
				olsMultipleLinearRegression = new OLSMultipleLinearRegression();
			} else {
				simpleRegression = new SimpleRegression();
			}
		}
	}

	/**
	 * This method sets the intercept for the multiple regression model.
	 * 
	 * @param value
	 */
	private void setIntercept(boolean value)
			throws MRMLinearModelNotFoundException {
		if (null == olsMultipleLinearRegression) {
			mrmDevLogger.info(MRMIndexes.getValue(EXC_MSG));
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			olsMultipleLinearRegression.setNoIntercept(value);
		}
	}

	/**
	 * This method reads the string and take the numbers inside the string and
	 * returns the number back.
	 * 
	 * @param value
	 * @return number
	 */
	private double parseString(String value) {
		StringBuilder number = new StringBuilder();
		if (null != value) {
			for (int i = 0; i < value.length(); i++) {
				if (value.charAt(i) >= '0' && value.charAt(i) <= '9') {
					number.append(value.charAt(i));
				}
			}
		}
		return Double.parseDouble(number.toString());
	}

	/**
	 * This will return the intercept value for simple regression model.
	 * 
	 * @return intercept
	 */
	public double getIntercept() throws MRMLinearModelNotFoundException {
		double intercept;
		if (null == simpleRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			intercept = simpleRegression.getIntercept();
		}
		return intercept;
	}

	/**
	 * This will return coefficient value for the simple regression model.
	 * 
	 * @return slope
	 */
	public double getCoefficient() throws MRMLinearModelNotFoundException {
		double slope;
		if (null == simpleRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			slope = simpleRegression.getSlope();
		}
		return slope;
	}

	/**
	 * This method returns the model coefficients.
	 * 
	 * @return coefficients
	 */
	public List<Double> getCoefficients()
			throws MRMLinearModelNotFoundException {
		List<Double> coefficients = new ArrayList<>();
		if (null == olsMultipleLinearRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			double[] beta = olsMultipleLinearRegression
					.estimateRegressionParameters();
			for (double value : beta) {
				coefficients.add(value);
			}
		}
		return coefficients;
	}

	/**
	 * This method will return the r-square value from the simple regression
	 * model or OLS multiple regression model.
	 * 
	 * @return rsquare
	 */
	public double getRSquare() {
		double rsquare;
		if (null != simpleRegression) {
			rsquare = simpleRegression.getRSquare();
		} else {
			rsquare = null != olsMultipleLinearRegression ? olsMultipleLinearRegression
					.calculateRSquared() : 0.0;
		}
		return rsquare;
	}

	/**
	 * This method will returns the p-value from the simple regression model.
	 * 
	 * @return pvalue
	 */
	public double getPvalue() throws MRMLinearModelNotFoundException {
		double pvalue;
		if (null == simpleRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			pvalue = simpleRegression.getSignificance();
		}
		return pvalue;
	}

	/**
	 * This method will return the p-values based on the OLS multiple linear
	 * regression model.
	 * 
	 * @return pvalues
	 */
	public double[] getPvalues() throws MRMLinearModelNotFoundException {
		double pvalues[] = null;
		if (null == olsMultipleLinearRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			double[] beta = olsMultipleLinearRegression
					.estimateRegressionParameters();
			double[] standardErrors = new double[beta.length];
			try {
				standardErrors = olsMultipleLinearRegression
						.estimateRegressionParametersStandardErrors();
				int residualdf = olsMultipleLinearRegression
						.estimateResiduals().length - beta.length;
				pvalues = new double[beta.length];
				for (int i = 0; i < beta.length; i++) {
					double tstat = beta[i] / standardErrors[i];
					double pvalue = new TDistribution(residualdf)
							.cumulativeProbability(-FastMath.abs(tstat)) * 2;
					pvalues[i] = pvalue;
				}
			} catch (Exception exception) {
				mrmDevLogger.error(exception.getMessage());
			}
		}
		return pvalues;
	}

	/**
	 * This method gives you the residual values based on the OLS multiple
	 * linear regression model.
	 * 
	 * @return residuals
	 */
	public double[] getResiduals() throws MRMLinearModelNotFoundException {
		double[] residuals;
		if (null == olsMultipleLinearRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			residuals = olsMultipleLinearRegression.estimateResiduals();
		}
		return residuals;
	}

	/**
	 * This method gives you the parameter standard errors values based on the
	 * OLS multiple linear regression model.
	 * 
	 * @return standardErrors
	 */
	public double[] getParameterStandardErrors()
			throws MRMLinearModelNotFoundException {
		double[] standardErrors;
		if (null == olsMultipleLinearRegression) {
			throw new MRMLinearModelNotFoundException(MRMIndexes.getValue(EXC_MSG));
		} else {
			standardErrors = olsMultipleLinearRegression
					.estimateRegressionParametersStandardErrors();
		}
		return standardErrors;
	}

	/**
	 * This method returns the simple linear model.
	 * 
	 * @return simpleRegression
	 */
	public SimpleRegression getSimpleModel() {
		return null != simpleRegression ? simpleRegression : null;
	}

	/**
	 * 
	 * @return
	 */
	public OLSMultipleLinearRegression getMultipleLinearModel() {
		return null != olsMultipleLinearRegression ? olsMultipleLinearRegression
				: null;
	}

	/**
	 * This method takes the independent variable values and predict the fitted
	 * values and return back.
	 * 
	 * @param indepVarValue
	 * @return fittedValues
	 */
	public double[] getFittedValue(double[] indepVarValue) {
		double[] fittedValues = null;

		if (null != indepVarValue) {
			fittedValues = new double[indepVarValue.length];
			if (null != simpleRegression) {
				for (int i = 0; i < indepVarValue.length; i++) {
					fittedValues[i] = simpleRegression
							.predict(indepVarValue[i]);
				}
			}
		}
		return fittedValues;
	}

	/**
	 * This method takes the dependent variable and residual values and provides
	 * the fitted values for OLSMultiple regression model.
	 * 
	 * @param dependantVariables
	 * @param residuals
	 * @return
	 */
	public double[] getFittedValues(double[] dependantVariables,
			double[] residuals) {
		double[] fittedValues = null;
		if (null != dependantVariables && null != residuals
				&& dependantVariables.length == residuals.length) {
			fittedValues = new double[dependantVariables.length];
			if (null != olsMultipleLinearRegression) {
				for (int i = 0; i < dependantVariables.length; i++) {
					fittedValues[i] = dependantVariables[i] - residuals[i];
				}
			}
		}
		return fittedValues;
	}

	/**
	 * This method takes the independent variable value and predict the fitted
	 * value and return back.
	 * 
	 * @param indepVarValue
	 * @return fittedValue
	 */
	public double getFittedValue(double indepVarValue) {
		double fittedValue;
		if (null == simpleRegression) {
			fittedValue = 0.0;
		} else {
			fittedValue = simpleRegression.predict(indepVarValue);
		}
		return fittedValue;
	}
}
