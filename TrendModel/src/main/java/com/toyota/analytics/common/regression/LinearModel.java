/**
 * 
 */
package com.toyota.analytics.common.regression;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.util.FastMath;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.util.TrendingModelUtil;

/**
 * This class contains methods which will do regression and gives the regression
 * output. This class contains the constructor which will returns the
 * corresponding model object based on the dependent, independent variables and
 * input data like in the form of data frame or double arrays.If we do want
 * intercept we can pass intercept is true for the model for the constructors.
 * By default it will be false.
 * 
 * This class has different method like getting the intercept, getting the
 * coefficients once done the regression, getting the p-values, r-square value
 * and also getting parameter standard errors, fitted values.
 * 
 * @author Naresh
 *
 */
public class LinearModel implements Serializable {

	private static final long serialVersionUID = -552015899173175824L;

	// Initializing the logger.
	static final Logger logger = Logger.getLogger(LinearModel.class);

	private static final String EXC_MSG = "MODEL_EXCE";

	// Initializing the OLSMultipleLinear Regression and SimpleRegression.
	private transient OLSMultipleLinearRegression olsMultipleLinearRegression;
	private SimpleRegression simpleRegression;

	/**
	 * This constructor will takes the parameters x and y, based on these
	 * parameters will provide the simple regression model and gives you the
	 * calculated values by providing the getter methods inside this.Here, x is
	 * independent variable, y is dependent variable.
	 * 
	 * @param x
	 * @param y
	 */
	public LinearModel(double[] x, double[] y) {
		if (null == simpleRegression) {
			logger.info("Simple Linear Regression with x, y is call started.");
			simpleRegression = new SimpleRegression();
			if (null != x && x.length > 0 && null != y && y.length > 0) {
				for (int i = 0; i < x.length; i++) {
					simpleRegression.addData(x[i], y[i]);
				}
			}
			logger.info("Simple Linear Regression with x, y is call ended.");
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
			logger.info("Simple Linear Regression with x, y and intercept is call started.");
			simpleRegression = new SimpleRegression(intercept);
			if (null != x && x.length > 0 && null != y && y.length > 0) {
				for (int i = 0; i < x.length; i++) {
					simpleRegression.addData(x[i], y[i]);
				}
			}
			logger.info("Simple Linear Regression with x, y and intercept is call ended.");
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
		logger.info("LinearModel param y, x call started at " + new Date());
		if (null != x && x.length > 0 && null != y && y.length > 0) {
			olsMultipleLinearRegression = new OLSMultipleLinearRegression();
			olsMultipleLinearRegression.newSampleData(y, x);
		}
		logger.info("LinearModel param y, x call ended at " + new Date());
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
		logger.info("LinearModel param y, x, intercept call started at "
				+ new Date());
		if (null != x && x.length > 0 && null != y && y.length > 0) {
			olsMultipleLinearRegression = new OLSMultipleLinearRegression();
			try {
				setIntercept(intercept);
				olsMultipleLinearRegression.newSampleData(y, x);
			} catch (AnalyticsRuntimeException exception) {
				logger.error(exception.getMessage());
			}
		}
		logger.info("LinearModel param y, x, intercept call ended at "
				+ new Date());
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
	 * @param dependentVariable
	 * @param independentVariables
	 * @param intercept
	 * @throws HeaderNotFoundException
	 * @throws LinearModelNotFoundException
	 */
	public LinearModel(DataFrame dataFrame, String dependentVariable,
			List<String> independentVariables, boolean intercept)
			throws AnalyticsRuntimeException {
		logger.info("LinearModel dataframe, dependentVariable, independentVariables call started at "
				+ new Date());
		if (null == dataFrame || null == dependentVariable
				|| independentVariables.isEmpty()) {
			logger.info("Please provide the dataFrame, dependentVariable, independentVariables values properly.");
		} else {
			// Reading the data frame data as Java RDD of Strings.
			JavaRDD<String> regressionDataSet = readDataset(dataFrame);

			// Collect the header information from the Java RDD as a map.
			Map<String, Integer> headersInformation = getHeadersInformation(regressionDataSet);
			logger.info("headersInformation " + headersInformation);

			// Adding the dependent variable is in first position.
			independentVariables.add(0, dependentVariable);

			// Taking the column indexes for particular columns based on the
			// header information.
			List<Integer> indexValues = getColumnIndexes(independentVariables,
					headersInformation);
			logger.info("Column index values " + indexValues);

			// Reading the data based the required column indexes.
			JavaRDD<String[]> listOfValues = readDataRDD(regressionDataSet,
					indexValues);
			List<String[]> listOfArrays = listOfValues.collect();

			if (null != listOfArrays) {
				// Removing the header information before going to regression.
				listOfArrays.remove(0);
				logger.info("After removing the regression input. "
						+ indexValues);

				// Taking number of rows.
				int numberOfRows = (int) regressionDataSet.count();
				logger.info("Number of rows in the RDD: " + numberOfRows
						+ " : dependentVariable: " + dependentVariable
						+ " : independentVariables: " + independentVariables);

				// Calling the regression based on the dependent variable and
				// independent variables.
				callRegression(listOfArrays, dependentVariable,
						independentVariables, intercept);
			}
		}
		logger.info("LinearModel dataframe, dependentVariable, independentVariables call ended at "
				+ new Date());
	}

	/**
	 * This method checks the model type based on the size of the list.
	 * 
	 * @param dependentVariable
	 * @param independentVariables
	 * @param y
	 * @param x
	 * @param intercept
	 * @throws LinearModelNotFoundException
	 */
	private void checkModelType(String dependentVariable,
			List<String> independentVariables, double[] y, double[][] x,
			boolean intercept) throws AnalyticsRuntimeException {
		logger.info("checkModelType with depVar, indepVars, y, and x call started at "
				+ new Date());
		generateModelInstance(dependentVariable, independentVariables);
		if (null != olsMultipleLinearRegression) {
			if (intercept) {
				olsMultipleLinearRegression.setNoIntercept(intercept);
				olsMultipleLinearRegression.newSampleData(y, x);
			} else {
				olsMultipleLinearRegression.newSampleData(y, x);
			}
		} else {
			simpleRegression = new SimpleRegression();
			for (int i = 0; i < y.length; i++) {
				simpleRegression.addData(x[i][0], y[i]);
			}
		}
		logger.info("checkModelType with depVar, indepVars, y, and x call ended at "
				+ new Date());
	}

	/**
	 * This method will create the model based on the input list size.
	 * 
	 * @param depVar
	 * @param indepVars
	 */
	private void generateModelInstance(String depVar, List<String> indepVars) {
		logger.info("generateModelInstance with depVar, indepVars call started at "
				+ new Date());
		if (null != depVar && null != indepVars && !indepVars.isEmpty()) {
			if (indepVars.size() > 1) {
				logger.info("olsMultipleLinearRegression with depVar, indepVars call at "
						+ new Date());
				olsMultipleLinearRegression = new OLSMultipleLinearRegression();
			} else {
				logger.info("simpleRegression with depVar, indepVars call at "
						+ new Date());
				simpleRegression = new SimpleRegression();
			}
		}
		logger.info("generateModelInstance with depVar, indepVars call ended at "
				+ new Date());
	}

	/**
	 * This method sets the intercept for the multiple regression model.
	 * 
	 * @param value
	 */
	private void setIntercept(boolean value) throws AnalyticsRuntimeException {
		logger.info("setIntercept with value call started at " + new Date());
		if (null == olsMultipleLinearRegression) {
			logger.info(TrendingModelUtil.getValue(EXC_MSG));
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			olsMultipleLinearRegression.setNoIntercept(value);
		}
		logger.info("setIntercept with value call ended at " + new Date());
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
		double finalValue = 0.0;
		if (null != value && !"".equals(value)) {
			for (int i = 0; i < value.length(); i++) {
				if (value.charAt(i) >= '0' && value.charAt(i) <= '9') {
					number.append(value.charAt(i));
				}
			}
			try {
				if (!"".equals(number.toString())) {
					finalValue = Integer.parseInt(number.toString());
				}
			} catch (Exception exception) {
				logger.error(exception.getMessage());
			}
		}
		return finalValue;
	}

	/**
	 * This will return the intercept value for simple regression model.
	 * 
	 * @return intercept
	 */
	public double getIntercept() throws AnalyticsRuntimeException {
		logger.info("getIntercept call started at " + new Date());
		double intercept;
		if (null == simpleRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			intercept = simpleRegression.getIntercept();
		}
		logger.info("getIntercept call ended at " + new Date());
		return intercept;
	}

	/**
	 * This will return coefficient value for the simple regression model.
	 * 
	 * @return slope
	 */
	public double getCoefficient() throws AnalyticsRuntimeException {
		logger.info("getCoefficient call started at " + new Date());
		double slope;
		if (null == simpleRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			slope = simpleRegression.getSlope();
		}
		logger.info("getCoefficient call ended at " + new Date());
		return slope;
	}

	/**
	 * This method returns the model coefficients.
	 * 
	 * @return coefficients
	 */
	public List<Double> getCoefficients() throws AnalyticsRuntimeException {
		logger.info("getCoefficients call started at " + new Date());
		List<Double> coefficients = new ArrayList<>();
		if (null == olsMultipleLinearRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			double[] beta = olsMultipleLinearRegression
					.estimateRegressionParameters();
			if (null != beta) {
				for (double value : beta) {
					coefficients.add(value);
					logger.info(" Coefficient Value " + value);
				}
			}
		}
		logger.info("getCoefficients call ended at " + new Date());
		return coefficients;
	}

	/**
	 * This method will return the r-square value from the simple regression
	 * model or OLS multiple regression model.
	 * 
	 * @return rsquare
	 */
	public double getRSquare() {
		logger.info("getRSquare call started at " + new Date());
		double rsquare;
		if (null != simpleRegression) {
			rsquare = simpleRegression.getRSquare();
		} else {
			rsquare = null != olsMultipleLinearRegression ? olsMultipleLinearRegression
					.calculateRSquared() : 0.0;
		}
		logger.info("getRSquare call ended at " + new Date());
		return rsquare;
	}

	/**
	 * This method will returns the p-value from the simple regression model.
	 * 
	 * @return pvalue
	 */
	public double getPvalue() throws AnalyticsRuntimeException {
		logger.info("getPvalue call started at " + new Date());
		double pvalue;
		if (null == simpleRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			pvalue = simpleRegression.getSignificance();
		}
		logger.info("getPvalue call ended at " + new Date());
		return pvalue;
	}

	/**
	 * This method will return the p-values based on the OLS multiple linear
	 * regression model.
	 * 
	 * @return pvalues
	 */
	public double[] getPvalues() throws AnalyticsRuntimeException {
		logger.info("getPvalues call started at " + new Date());
		double pvalues[] = null;
		if (null == olsMultipleLinearRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
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
				logger.error(exception.getMessage());
			}
		}
		logger.info("getPvalues call ended at " + new Date());
		return pvalues;
	}

	/**
	 * This method gives you the residual values based on the OLS multiple
	 * linear regression model.
	 * 
	 * @return residuals
	 */
	public double[] getResiduals() throws AnalyticsRuntimeException {
		logger.info("getResiduals call started at " + new Date());
		double[] residuals;
		if (null == olsMultipleLinearRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			residuals = olsMultipleLinearRegression.estimateResiduals();
		}
		logger.info("getResiduals call ended at " + new Date());
		return residuals;
	}

	/**
	 * This method gives you the parameter standard errors values based on the
	 * OLS multiple linear regression model.
	 * 
	 * @return standardErrors
	 */
	public double[] getParameterStandardErrors()
			throws AnalyticsRuntimeException {
		logger.info("getParameterStandardErrors call started at " + new Date());
		double[] standardErrors;
		if (null == olsMultipleLinearRegression) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue(EXC_MSG));
		} else {
			standardErrors = olsMultipleLinearRegression
					.estimateRegressionParametersStandardErrors();
		}
		logger.info("getParameterStandardErrors call ended at " + new Date());
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
	 * @param independentVariableValue
	 * @return fittedValues
	 */
	public double[] getFittedValue(double[] independentVariableValue) {
		logger.info("getFittedValue call started at " + new Date());
		double[] fittedValues = null;

		if (null != independentVariableValue) {
			fittedValues = new double[independentVariableValue.length];
			if (null != simpleRegression) {
				for (int i = 0; i < independentVariableValue.length; i++) {
					fittedValues[i] = simpleRegression
							.predict(independentVariableValue[i]);
				}
			}
		}
		logger.info("getFittedValue call ended at " + new Date());
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
		logger.info("getFittedValues call started at " + new Date());
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
		logger.info("getFittedValues call ended at " + new Date());
		return fittedValues;
	}

	/**
	 * This method takes the independent variable value and predict the fitted
	 * value and return back.
	 * 
	 * @param independentVariableValue
	 * @return fittedValue
	 */
	public double getFittedValue(double independentVariableValue) {
		logger.info("getFittedValue call started at " + new Date());
		double fittedValue;
		if (null == simpleRegression) {
			fittedValue = 0.0;
		} else {
			fittedValue = simpleRegression.predict(independentVariableValue);
		}
		logger.info("getFittedValue call ended at " + new Date());
		return fittedValue;
	}

	/**
	 * This method calls the regression based on the data, dependent variable
	 * and independent variables list. Here, based on the input data construct
	 * the matrix and check the model based on the size of the variables and run
	 * the regression accordingly.
	 * 
	 * @param listOfValues
	 * @param noOfLines
	 * @param requiredColumnsList
	 * @param intercept
	 * @throws LinearModelNotFoundException
	 * 
	 */
	private void callRegression(List<String[]> listOfValues,
			String dependentVariable, List<String> requiredColumnsList,
			boolean intercept) throws AnalyticsRuntimeException {
		logger.info("callRegression started at " + new Date());

		// Reading the number of lines in the input.
		int noOfLines = listOfValues.size();
		logger.info("noOfLines: " + noOfLines + " : dependentVariable: "
				+ dependentVariable + " : requiredColumnsList: "
				+ requiredColumnsList);

		// Building the matrix.
		String[][] csvMatrix = listOfValues.toArray(new String[noOfLines][]);

		// Number of columns in the data set.
		int noOfColumns = requiredColumnsList.size();

		// Creating the arrays based on the number of rows and number of column
		// sizes.
		double[][] x = new double[noOfLines][noOfColumns - 1];
		double[] y = new double[noOfLines];
		if (null != csvMatrix) {
			int dCol = 0;

			// Reading the input data and set to arrays x and y.
			for (int i = 0; i < noOfLines; i++) {
				for (int j = 0; j < noOfColumns; j++) {
					if (j < dCol && null != csvMatrix[i][j]) {
						try {
							x[i][j] = Double.parseDouble(csvMatrix[i][j]);
						} catch (Exception exception) {
							x[i][j] = parseString(csvMatrix[i][j]);
						}
					}
					if (j > dCol && null != csvMatrix[i][j]) {
						try {
							x[i][j - 1] = Double.parseDouble(csvMatrix[i][j]);
						} catch (Exception exception) {
							x[i][j - 1] = parseString(csvMatrix[i][j]);
						}
					}
				}
				if (null != csvMatrix[i][dCol]) {
					try {
						y[i] = Double.parseDouble(csvMatrix[i][dCol]);
					} catch (Exception exception) {
						y[i] = parseString(csvMatrix[i][dCol]);
					}
				}
			}

			// Checking the model type whether it is simple or multiple liner
			// model.
			requiredColumnsList.remove(0);
			logger.info("requiredColumnsList: " + requiredColumnsList);
			checkModelType(dependentVariable, requiredColumnsList, y, x,
					intercept);
		}
		logger.info("callRegression ended at " + new Date());
	}

	/**
	 * This method reads the data frame header like column names and reads data
	 * transform it as Java RDD of string.
	 * 
	 * @param dat
	 * @return
	 * @throws HeaderNotFoundException
	 */
	private JavaRDD<String> readDataset(DataFrame dat)
			throws AnalyticsRuntimeException {
		logger.info("Number of lines are: " + dat.count());
		JavaRDD<String> colheader;

		// Reading the frame column header.
		colheader = readFrameHeader(dat);
		logger.info("Frame header: " + colheader);

		// Check the header data.
		if (null == colheader || colheader.isEmpty()) {
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue("COLUMN_HEADER_info"));
		} else {
			// Reading the Data frame data.
			JavaRDD<String> data = dat.toJavaRDD().map(
					new Function<Row, String>() {
						private static final long serialVersionUID = 2453646105748597983L;

						@Override
						public String call(Row row) throws Exception {
							return row.mkString(",");
						}
					});
			colheader = colheader.union(data);
		}
		logger.info("Frame Data: " + colheader);
		return colheader;
	}

	/**
	 * This method reads the columns from the data frame and transform into Java
	 * RDD, return this RDD back.
	 * 
	 * @param dat
	 */
	private JavaRDD<String> readFrameHeader(DataFrame dat) {
		JavaRDD<String> colheader;
		String finalString = "";

		// Taking the columns from the data frame.
		String[] datacol = dat.columns();
		for (int d = 0; d < datacol.length; d++) {
			if (!finalString.equals("")) {
				finalString = finalString + ",";
			}
			finalString = finalString + datacol[d];
		}
		logger.info("Column header: " + finalString);
		colheader = TrendingModelUtil.getJavaSparkContext().parallelize(
				Arrays.asList(finalString));
		return colheader;
	}

	/**
	 * This method returns the column headers information. This method will give
	 * the column name as key and particular column index as value.
	 * 
	 * @param dataRDD
	 * @return
	 * @throws HeaderNotFoundException
	 */
	public static Map<String, Integer> getHeadersInformation(
			JavaRDD<String> dataRDD) throws AnalyticsRuntimeException {
		Map<String, Integer> columnsHeadersMap = new LinkedHashMap<>();
		String headerLine = dataRDD.first();
		String lowerHeader = headerLine.toLowerCase();
		String headerValue = checkHeader(lowerHeader);
		if (null != headerValue && headerValue.length() > 3) {
			throw new AnalyticsRuntimeException("No header information found.");
		} else {
			if (null == lowerHeader) {
				logger.info("No column header.");
			} else {
				String[] columnHeaders = StringUtils.split(lowerHeader, ",");
				for (int i = 0; i < columnHeaders.length; i++) {
					columnsHeadersMap.put(columnHeaders[i], i);
				}
			}
		}
		return columnsHeadersMap;
	}

	/**
	 * This method returns index values based on the column names.
	 * 
	 * @param requiredColumnNames
	 * @param headersInformation
	 * @return requiredData
	 */
	public static List<Integer> getColumnIndexes(
			List<String> requiredColumnNames,
			Map<String, Integer> headersInformation) {
		List<Integer> requiredData = new ArrayList<>();
		if (null == requiredColumnNames || requiredColumnNames.isEmpty()
				|| null == headersInformation || headersInformation.isEmpty()) {
			logger.info("No column header.");
		} else {
			for (String columnName : requiredColumnNames) {
				String lowerColumn = columnName.trim().toLowerCase();
				if (headersInformation.containsKey(lowerColumn)) {
					requiredData.add(headersInformation.get(lowerColumn));
				}
			}
		}
		return requiredData;
	}

	/**
	 * This method parse the string and returns the number from that.
	 * 
	 * @param headerLine
	 * @return number
	 */
	public static String checkHeader(String headerLine) {
		StringBuilder number = new StringBuilder();
		if (null != headerLine && !"".equals(headerLine)) {
			for (int i = 0; i < headerLine.length(); i++) {
				if (headerLine.charAt(i) >= '0' && headerLine.charAt(i) <= '9') {
					number.append(headerLine.charAt(i));
				}
			}
		}
		return number.toString();
	}

	/**
	 * This method reads the data based on the indexes of the columns. This
	 * method reads the data line by line based on the indexes of the particular
	 * columns.
	 * 
	 * @param dataRDD
	 * @param requiredColumnIndexes
	 * @return
	 */
	private JavaRDD<String[]> readDataRDD(JavaRDD<String> dataRDD,
			final List<Integer> requiredColumnIndexes) {
		return dataRDD.map(new Function<String, String[]>() {
			private static final long serialVersionUID = 4114283967236072616L;

			@Override
			public String[] call(String line) {
				// Each line split into words by comma seperator.
				String[] lineData = org.apache.commons.lang.StringUtils.split(
						line, ",");
				String[] requiredData = null;
				if (!requiredColumnIndexes.isEmpty()) {
					requiredData = new String[requiredColumnIndexes.size()];
					for (Integer integer : requiredColumnIndexes) {
						requiredData[requiredColumnIndexes.indexOf(integer)] = lineData[integer];
					}
				}
				return requiredData;
			}
		});
	}
}
