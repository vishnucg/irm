package com.toyota.analytix.common.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;

/**
 * This class contains utilities which will gives like reading the data set
 * header information, getting column indexes based on the column names. Here
 * one of the utility is to get the numbers from the string from the input data
 * set. One more utility which will read the properties file and take the values
 * based on the key.
 * 
 * @author 
 *
 */
public class MRMUtil {

	static final Logger mrmDevLogger = Logger.getLogger(MRMUtil.class);

	/**
	 * This method returns the column headers information. This method will give
	 * the column name as key and particular column index as value.
	 * 
	 * @param dataRDD
	 * @return
	 * @throws MRMHeaderNotFoundException
	 */
	public static Map<String, Integer> getHeadersInformation(JavaRDD<String> dataRDD) {
		mrmDevLogger.trace(">>> getHeadersInformation");
		Map<String, Integer> columnsHeadersMap = new LinkedHashMap<>();
		String headerLine = dataRDD.first();
		String lowerHeader = headerLine.toLowerCase();
		String headerValue = checkHeader(lowerHeader);
		if (null != headerValue && headerValue.length() > 3) {
			mrmDevLogger.error(MRMUtil.getValue("COLUMN_HEADER_WARN"));
			throw new AnalytixRuntimeException(MRMUtil.getValue("COLUMN_HEADER_WARN"));
		} else {
			if (null == lowerHeader) {
				mrmDevLogger.warn(MRMUtil.getValue("COLUMN_HEADER_WARN"));
			} else {
				String[] columnHeaders = lowerHeader.split(",");
				for (int i = 0; i < columnHeaders.length; i++) {
					columnsHeadersMap.put(columnHeaders[i], i);
				}
			}
			mrmDevLogger.trace("Header Information: " + columnsHeadersMap);
		}
		mrmDevLogger.trace("<<< getHeadersInformation");
		return columnsHeadersMap;
	}

	/**
	 * This method returns index values based on the column names.
	 * 
	 * @param requiredColumnNames
	 * @param headersInformation
	 * @return
	 */
	public static List<Integer> getColumnIndexes(List<String> requiredColumnNames,
			Map<String, Integer> headersInformation) {
		String me = "getColumnIndexes";
		mrmDevLogger.info(me);
		List<Integer> requiredData = new ArrayList<>();
		if (null == requiredColumnNames || requiredColumnNames.isEmpty() || null == headersInformation
				|| headersInformation.isEmpty()) {
			mrmDevLogger.warn(MRMUtil.getValue("COLUMN_HEADER_WARN"));
		} else {
			for (String columnName : requiredColumnNames) {
				String columnValue = columnName.trim();
				String lowerColumn = columnValue.toLowerCase();
				if (headersInformation.containsKey(lowerColumn)) {
					requiredData.add(headersInformation.get(lowerColumn));
				}
			}
		}
		mrmDevLogger.info(requiredData);
		return requiredData;
	}

	/**
	 * This method parse the string and returns the number from that.
	 * 
	 * @param s
	 * @return
	 */
	public static double parseString(String s) {
		StringBuilder number = new StringBuilder();
		double finalValue = 0.0;
		if (null != s && !"".equals(s)) {
			for (int i = 0; i < s.length(); i++) {
				if (s.charAt(i) >= '0' && s.charAt(i) <= '9') {
					number.append(s.charAt(i));
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
	 * @return
	 */
	public static String getValue(String key) {
		return QueryProperties.getValue(key);
	}

	/**
	 * This method parse the string and returns the number from that.
	 * 
	 * @param headerLine
	 * @return
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

	// TODO parallelize Percentile calculation
	// public static double[] getPercentiles(JavaRDD<Double> rdd, double[]
	// percentiles, long rddSize, int numPartitions) {
	// double[] values = new double[percentiles.length];
	//
	// JavaRDD<Double> sorted = rdd.sortBy((Double d) -> d, true,
	// numPartitions);
	// JavaPairRDD<Long, Double> indexed =
	// sorted.zipWithIndex().mapToPair((Tuple2<Double, Long> t) -> t.swap());
	//
	// for (int i = 0; i < percentiles.length; i++) {
	// double percentile = percentiles[i];
	// long id = (long) (rddSize * percentile);
	// values[i] = indexed.lookup(id).get(0);
	// }
	//
	// return values;
	// }

	public static double[] getPercentiles(DataFrame dataFrame, String column, double[] buckets) {
		// Select and sort on the column
		DataFrame columnDataFrame = dataFrame.select(column).sort(column);
		mrmDevLogger.trace(">>> getPercentiles");
		Row[] rowspercent = columnDataFrame.collect();

		double[] percentiles = new double[buckets.length];

		/*
		 * create a outlier values for the sales share.Finding the buckets
		 * values for sales share column.
		 */
		for (int i = 0; i < buckets.length; i++) {
			double m = buckets[i] * (rowspercent.length + 1);
			int elementPosition = (int) m - 1;
			//mrmDevLogger.trace("TOTAL elementPosition TO position: 0 " + elementPosition);
			if (elementPosition >= rowspercent.length - 1) {
				percentiles[i] = rowspercent[rowspercent.length - 1].getDouble(0);
			} else {
				if (m < 1) {
					percentiles[i] = rowspercent[0].getDouble(0);
				} else {
					percentiles[i] = rowspercent[elementPosition].getDouble(0)
							+ (rowspercent[elementPosition + 1].getDouble(0)
									- rowspercent[elementPosition].getDouble(0)) * (m % 1);
				}
			}
		}

		// Print the percentiles
		StringBuffer bBuffer = new StringBuffer();
		StringBuffer pBuffer = new StringBuffer();
		for (int i = 0; i < buckets.length; i++) {
			pBuffer.append(String.format("%.12f", percentiles[i]));
			bBuffer.append(String.format("%.12f", buckets[i]));
			pBuffer.append("\t");
			bBuffer.append("\t");
		}
		mrmDevLogger.debug("<<< getPercentiles: PERCENTILES FOR COLUMN:" + column + "\n" + bBuffer.toString() + "\n" + pBuffer.toString());

		return percentiles;
	}

	public static DataFrame applyPercentileScoresToColumns(DataFrame df, double[] percentileBuckets,
			double[] col1Percentiles, double[] percentile2, String column1, String column2) {
		mrmDevLogger.debug(">>> applyPercentileScoresToColumns");

		String[] columns = df.columns();
		Set<String> allColumns = new HashSet<String>(Arrays.asList(columns));
		Set<String> selectedColumns = new HashSet<String>(Arrays.asList(new String[] { column1, column2 }));
		allColumns.removeAll(selectedColumns);

		getCommaSeperatedString(allColumns);

		for (int i = 0; i < percentileBuckets.length / 2; i++) {
			String[] queryArray = new String[] {
					"case when " + column1 + " = 100 and ((SALES_SHARE < " + String.format("%.12f", col1Percentiles[i])
							+ ") or (" + "SALES_SHARE > "
							+ String.format("%.12f", col1Percentiles[(percentileBuckets.length - i - 1)]) + ")) then  "
							+ (percentileBuckets[i] * 100) + " else "+ column1 +" end as "+ column1,
					"case when "+column2 +" = 100 and ((MSRP_COMP_RATIO < "
							+ String.format("%.12f", percentile2[i]) + ") or (" + "MSRP_COMP_RATIO > "
							+ String.format("%.12f", percentile2[percentileBuckets.length - i - 1]) + ")) then "
							+ (percentileBuckets[i] * 100)
							+ " else "+column2+" end as "+column2 };

			queryArray = (String[]) ArrayUtils.addAll(queryArray, allColumns.toArray());
			mrmDevLogger.debug("Query: " + Arrays.toString(queryArray));
			df = df.selectExpr(queryArray);
		}

		mrmDevLogger.debug("<<< applyPercentileScoresToColumns");
		return df;
	}

	public static String getCommaSeperatedString(Collection<String> allColumns) {
		StringBuffer sb = new StringBuffer();
		// assume String
		Iterator<String> it = allColumns.iterator();
		if (it.hasNext()) {
			sb.append(it.next());
		}
		while (it.hasNext()) {
			sb.append(", " + it.next());
		}
		return sb.toString();
	}

	public static String debugPrintMap(Map<?, ?> map) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		MapUtils.debugPrint (ps, "**** Map ****", map);
		String content = new String(baos.toByteArray(), StandardCharsets.UTF_8);
		
		try {
			ps.close();
			baos.close();
		} catch (IOException e) {
		}
		return content;
	}
	
	/**
	 * Enumerate a column in the dataset
	 * Adds new rows to the dataframe with enumeration on the row values
	 * 
	 * @param inputTable
	 * @return
	 */
	public static DataFrame enumerateColumns(DataFrame inputTable, String... columns) {
		
		mrmDevLogger.info("Enumerating columns: " + Arrays.toString(columns));
		//if columns not found throw error
		String[] allColumns = inputTable.columns();
		List<String> notFoundColumns = new ArrayList<String>();

		for (String column : columns) {
			if (!ArrayUtils.contains(allColumns, column))
				notFoundColumns.add (column);
		}
		
		if(notFoundColumns.size() > 0){
			throw new AnalytixRuntimeException("Cannot enumerate Dataframe. Columns not found: " + ArrayUtils.toString(notFoundColumns.toArray()));
		}

		//Needs to be done in order to avoid 1.3.0 bug
		//resolved attributes sales_share missing from 
		//https://www.mail-archive.com/search?l=user@spark.apache.org&q=subject:%22dataframe+can+not+find+fields+after+loading+from+hive%22&o=newest&f=1
		inputTable = inputTable.sqlContext().createDataFrame(inputTable.rdd(),
				inputTable.schema());

		Map<String, Map<String, Integer>> columnRowValuesEnumMap = new HashMap<String, Map<String, Integer>>();
		// for each column 
		for (String column : columns) {
			// sort the column and get values
			List<Row> rowarray = inputTable.select(column).distinct().sort(column).collectAsList();
	
			Map<String, Integer> rowValuesEnumMap = new HashMap<String, Integer>();
			
			// enumerate each value and create query string
			for (int i = 0; i < rowarray.size(); i++) {
				String rowValue = rowarray.get(i).getString(0);
				
				mrmDevLogger.debug("In enumerateColumns column:" + column + " rowValue:" + rowValue);

				// ignore if rowValue is null
				if (rowValue == null){
					throw new AnalytixRuntimeException("Null row for column:" + column + ". Cannot enumerate.");
				}
				//set the value of the integer (avoid 0)
				rowValuesEnumMap.put(rowValue, i+1);
			}
			columnRowValuesEnumMap.put(column, rowValuesEnumMap);

			//rename the column with orig
			inputTable = inputTable.withColumnRenamed(column, column+"_orig");
			
			//add a new column with actual column name
			inputTable = inputTable.withColumn(column, new Column(new Literal(10, DataTypes.IntegerType)));
		}

		// get all columns again to create the query for replacement
		allColumns = inputTable.columns();
		
		String[] queryArr = new String[allColumns.length];
		
		for (int i = 0; i< allColumns.length; i++) {
			String col = allColumns[i];
			//if column is in the enumerable columns then create case statement
			if (columnRowValuesEnumMap.containsKey(col)){
				StringBuffer caseBuffer = new StringBuffer();
				caseBuffer.append("CASE ");
				Map<String, Integer> rowValuesEnumMap = columnRowValuesEnumMap.get(col);
				for (String row: rowValuesEnumMap.keySet()){
					caseBuffer.append(" WHEN " + col + "_orig ='" + row + "'  then " + rowValuesEnumMap.get(row));
				}
				//query.append(" ELSE '' END as " + col);
				caseBuffer.append("  END as " + col);
				queryArr[i] = caseBuffer.toString();
			}else{
				queryArr[i]=col;
			}
		}
		mrmDevLogger.debug("In enumerateColumns Query:"+ ArrayUtils.toString(queryArr));

		inputTable = inputTable.selectExpr(queryArr);
		return inputTable;
	}
	
}
