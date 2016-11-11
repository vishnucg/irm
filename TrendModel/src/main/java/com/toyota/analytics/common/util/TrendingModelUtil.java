/**
 * 
 */
package com.toyota.analytics.common.util;

import static org.apache.spark.sql.functions.avg;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.trend.trendcalculations.TrendCalculationsDTO;

/**
 * This class initializes HiveContext and JavaSparkContext. This contains the
 * static methods to get the spark context and hive context and other utility
 * methods. This contains the static block which will load all the contexts and
 * the resource bundle. This reads the spark configuration properties from
 * properties.conf file from spark-submit.
 * 
 * And also this class contain methods like getting the column header
 * information from data frame and read the column indexes. This contains the
 * method which will read all the dependent and independent variabels details
 * for the trend models and also gives the average of number of rows in the data
 * frame.
 * 
 * @author Naresh
 *
 */
public class TrendingModelUtil implements Serializable {

	private static final long serialVersionUID = 4771266244808602848L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(TrendingModelUtil.class);

	// Variables HiveContext and JavaSparkContext are initialized.
	public static final HiveContext hiveContext;
	public static final JavaSparkContext javaSparkContext;

	// Resource bundle is initialized.
	private static ResourceBundle resourceBundle;

	static {
		// Loading the resource bundle.
		resourceBundle = ResourceBundle.getBundle("messages");

		// Spark configuration properties.
		SparkConf sparkConf = new SparkConf();

		// Getting a java spark context object by using the spark configuration.
		javaSparkContext = new JavaSparkContext(sparkConf);

		sparkConf.setMaster("yarn-client");
		sparkConf.setAppName("TrendModel");

		// Creating a hive context object connection by using java spark
		// context object
		hiveContext = new org.apache.spark.sql.hive.HiveContext(
				JavaSparkContext.toSparkContext(javaSparkContext));
	}

	/**
	 * @return the hivecontext
	 */
	public static HiveContext getHivecontext() {
		return hiveContext;
	}

	/**
	 * This method returns the java spark context.
	 * 
	 * @return sc
	 */
	public static JavaSparkContext getJavaSparkContext() {
		return javaSparkContext;
	}

	/**
	 * This method closes the java spark context.
	 * 
	 */
	public static void closeJavaSparkContext() {
		javaSparkContext.close();
	}

	/**
	 * 
	 * This method load the properties file and get the value from the
	 * properties file based on the key.
	 * 
	 * @param key
	 * @return logValue
	 */
	public static String getValue(String key) {
		String logValue = null;
		try {
			logValue = null != resourceBundle.getString(key) ? resourceBundle
					.getString(key) : "";
		} catch (MissingResourceException exception) {
			logger.error(exception);
		}
		return logValue;
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
			throw new AnalyticsRuntimeException(getValue("COLUMN_HEADER_WARN"));
		} else {
			if (null == lowerHeader) {
				logger.info(getValue("COLUMN_HEADER_WARN"));
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
			logger.info(getValue("COLUMN_HEADER_WARN"));
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
	 * This method parse the string and returns the number from that.
	 * 
	 * @param value
	 * @return finalValue
	 */
	public static double parseString(String value) {
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
	 * This method reads the properties like dependent variable and independent
	 * variables for all models. These dependent and independent values are in
	 * the messages.properties file. These will be used in model regression.
	 * 
	 * @param propertiesValues
	 * @return
	 * @throws PropertiesNotFoundException
	 */
	public static TrendCalculationsDTO readProperties()
			throws AnalyticsRuntimeException {
		logger.info("readProperties started at " + new Date());
		TrendCalculationsDTO trendCalculationsDTO = new TrendCalculationsDTO();

		// Setting up the dependent variables for the regression.
		trendCalculationsDTO.setMsrpDependantVar(TrendingModelUtil
				.getValue("MSRP_DEP_VAR"));
		trendCalculationsDTO.setDealergrossDependantVar(TrendingModelUtil
				.getValue("DEALER_GROSS_DEP_VAR"));
		String incentiveDepVarList = TrendingModelUtil
				.getValue("INCENTIVE_DEP_VAR");
		String[] varValuesList = StringUtils.split(incentiveDepVarList, ",");
		List<String> listOfVariables = new ArrayList<>();
		for (String variable : varValuesList) {
			listOfVariables.add(variable.toLowerCase().trim());
		}
		trendCalculationsDTO.setDependentVarsList(listOfVariables);

		logger.info(trendCalculationsDTO.getMsrpDependantVar() + " : "
				+ trendCalculationsDTO.getDependentVarsList() + " : "
				+ trendCalculationsDTO.getDealergrossDependantVar());

		// Setting up the independent column list for the regression.
		String msrpIndepList = TrendingModelUtil
				.getValue("MSRP_INDEP_VAR_LIST");
		String[] msrpValuesList = StringUtils.split(msrpIndepList, ",");
		List<String> msrpVariables = new ArrayList<>();
		for (String variable : msrpValuesList) {
			msrpVariables.add(variable.toLowerCase().trim());
		}
		trendCalculationsDTO.setMsrpIndependentColumnList(msrpVariables);

		String incentiveIndepList = TrendingModelUtil
				.getValue("INCENTIVE_INDEP_VAR_LIST");
		String[] incentiveValuesList = StringUtils.split(incentiveIndepList,
				",");
		List<String> incentiveVariables = new ArrayList<>();
		for (String variable : incentiveValuesList) {
			incentiveVariables.add(variable.toLowerCase().trim());
		}
		trendCalculationsDTO
				.setIncentiveIndependentColumnList(incentiveVariables);
		String dealerGrossIndepList = TrendingModelUtil
				.getValue("DEALER_GROSS_INDEP_VAR_LIST");
		String[] dealerGrossList = StringUtils.split(dealerGrossIndepList, ",");
		List<String> dealerIndepVariables = new ArrayList<>();
		for (String variable : dealerGrossList) {
			dealerIndepVariables.add(variable.toLowerCase().trim());
		}
		trendCalculationsDTO
				.setDealergrossIndependentColumnList(dealerIndepVariables);
		logger.info(trendCalculationsDTO.getMsrpIndependentColumnList() + " : "
				+ trendCalculationsDTO.getIncentiveIndependentColumnList()
				+ " : "
				+ trendCalculationsDTO.getDealergrossIndependentColumnList());
		logger.info("readProperties ended at " + new Date());
		return trendCalculationsDTO;
	}

	/**
	 * This method calculates average based for number of rows in history frame
	 * for particular column.
	 * 
	 * @param historyFrame
	 * @param columnName
	 * @param numberOfRows
	 * @return
	 */
	public static double averageNumberOfRowsInFrame(DataFrame historyFrame,
			String columnName, int numberOfRows) {

		// Sorting history frame by business_month in descending order.
		DataFrame lastThreeRowsFrame = historyFrame
				.sort(historyFrame.col(
						TrendingModelUtil.getValue("BUSINESS_MONTH")).desc())
				.limit(numberOfRows).select(columnName);
		// Taking the average of n number of rows as per the request.
		Row row = lastThreeRowsFrame.agg(avg(historyFrame.col(columnName)))
				.first();
		return row.isNullAt(0) ? 0.0 : row.getDouble(0);
	}

	/**
	 * This method reads the final data frame and checks each and every column
	 * in the independent variables list whether that columns has same values or
	 * not. If the column has same values, then it removes from the list finally
	 * it returns the independent variables list.
	 * 
	 * @param finalFrame
	 * @param independentVariables
	 * @return
	 */
	public static List<String> checkColumnsData(DataFrame finalFrame,
			List<String> independentVariables) {
		// Collecting the final list which are having the same column values in
		// the list.
		List<String> finalList = new ArrayList<>();
		if (null != independentVariables && !independentVariables.isEmpty()) {
			for (String variable : independentVariables) {
				DataFrame columnData = finalFrame.select(
						finalFrame.col(variable)).distinct();
				if (columnData.count() < 2) {
					finalList.add(variable);
				}
			}

			// Removing the columns which are having the same values.
			for (String var : finalList) {
				independentVariables.remove(var);
			}
		}
		return independentVariables;
	}
}
