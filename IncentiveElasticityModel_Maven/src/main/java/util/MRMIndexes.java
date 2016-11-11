package util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import exceptions.MRMHeaderNotFoundException;

/**
 * This class contains utilities which will gives like reading the data set
 * header information, getting column indexes based on the column names. Here
 * one of the utility is to get the numbers from the string from the input data
 * set. One more utility which will read the properties file and take the values
 * based on the key.
 * 
 * @author Naresh
 *
 */
public class MRMIndexes {

	static final Logger mrmDevLogger = Logger.getLogger(MRMIndexes.class);

	private static ResourceBundle resourceBundle = null;

	// Loading the query properties file.
	static {
		resourceBundle = ResourceBundle.getBundle("Query");
	}
	

	/**
	 * This method returns the column headers information. This method will give
	 * the column name as key and particular column index as value.
	 * 
	 * @param dataRDD
	 * @return
	 * @throws MRMHeaderNotFoundException
	 */
	public static Map<String, Integer> getHeadersInformation(
			JavaRDD<String> dataRDD) throws MRMHeaderNotFoundException {
		Map<String, Integer> columnsHeadersMap = new LinkedHashMap<>();
		String headerLine = dataRDD.first();
		String lowerHeader = headerLine.toLowerCase();
		String headerValue = checkHeader(lowerHeader);
		if (null != headerValue && headerValue.length() > 3) {
			throw new MRMHeaderNotFoundException(
					MRMIndexes.getValue("COLUMN_HEADER_WARN"));
		} else {
			if (null == lowerHeader) {
				mrmDevLogger.warn(MRMIndexes.getValue("COLUMN_HEADER_WARN"));
			} else {
				String[] columnHeaders = lowerHeader.split(",");
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
			mrmDevLogger.warn(MRMIndexes.getValue("COLUMN_HEADER_WARN"));
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
				mrmDevLogger.error(exception.getMessage());
			}
		}
		return finalValue;
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
			if (null == key) {
				mrmDevLogger.info(key);
			} else {
				logValue = null != resourceBundle.getString(key) ? resourceBundle
						.getString(key) : "";
			}
		} catch (MissingResourceException exception) {
			mrmDevLogger.error(exception.getMessage());
		}
		return logValue;
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

}
