/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.regression.LinearModelDTO;
import com.toyota.analytics.common.util.TrendingModelUtil;

/**
 * This class contains the methods which will do MSRP trending calculations.Here
 * will do regression on the input series data set based on the dependent
 * variable (avg_msrp_amt_chg_pct) and independent variables list
 * (major_change,minor_change,new_model_year_flag) and get the intercept and
 * coefficient values for variables list. After we got the intercept and
 * coefficient values will predict the forecast MSRP values based on the
 * formula.
 * 
 * @author Naresh
 *
 */
public class MSRPTrendModelManager implements Serializable {

	private static final long serialVersionUID = 530965434956513510L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(MSRPTrendModelManager.class);

	/**
	 * This method run the regression on the data set based on the dependent
	 * variable and independent variables list and get the intercept and
	 * coefficient values.
	 * 
	 * @param historyDataFrame
	 * @param futureFrame
	 * @param previousMsrpValue
	 * @param dependentVar
	 * @param independentVariables
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public Map<String, List<Double>> calculateMsrpTrend(
			DataFrame historyDataFrame, DataFrame futureFrame,
			double previousMsrpValue, String dependentVar,
			List<String> independentVariables, int maxModelYear)
			throws IOException, AnalyticsRuntimeException {
		Map<String, List<Double>> forecastDealerGrossValues;
		// Run the regression and get the intercept and coefficient values.
		logger.info("MSRP Forecast Model callRegressionModel started.");
		independentVariables = TrendingModelUtil.checkColumnsData(
				historyDataFrame, independentVariables);

		logger.info("After check the data in frame: " + independentVariables);
		TrendModelManager trendModelManager = new TrendModelManager();
		LinearModelDTO linearModelDTO = trendModelManager.callRegressionModel(
				historyDataFrame, dependentVar, independentVariables, false);
		logger.info("MSRP Forecast Model callRegressionModel ended.");

		// Calculate predicted MSRP Values.
		logger.info("predictMsrpValues started.");
		forecastDealerGrossValues = predictMsrpValues(futureFrame,
				linearModelDTO, previousMsrpValue, maxModelYear);
		logger.info("predictMsrpValues ended.");
		return forecastDealerGrossValues;
	}

	/**
	 * This method predict the MSRP Values based on the intercept and
	 * coefficient values.
	 * 
	 * @param forecastDataFrame
	 * @param linearModelInformation
	 * @param previousMsrpValue
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private Map<String, List<Double>> predictMsrpValues(
			DataFrame forecastDataFrame, LinearModelDTO linearModelInformation,
			double previousMsrpValue, int maxModelYear) throws IOException,
			AnalyticsRuntimeException {

		// Initializing the lists for the predict and forecast values.
		List<Double> forecastMsrpValues = new ArrayList<>();
		List<Double> avgMsrpChgPctValues = new ArrayList<>();
		Map<String, List<Double>> msrpValues = new HashMap<>();

		// Reading the column coefficients list and intercept.
		Map<String, Double> values = linearModelInformation
				.getCoefficientValues();
		double intercept = linearModelInformation.getIntercept();
		logger.info("MSRP Values Intercept: " + intercept);
		double majorChangeCoefficient;
		double minorChangeCoefficient;
		double newModelYearFlagCoefficient;
		double regularChangeCoefficient;

		// Picking up required columns.
		DataFrame finalFrame = getMsrpForecastInput(forecastDataFrame.select(
				forecastDataFrame.col(TrendingModelUtil
						.getValue("CHANGE_CATEGORY")),
				forecastDataFrame.col(TrendingModelUtil
						.getValue("NEW_MODEL_YEAR_FLAG")), forecastDataFrame
						.col(TrendingModelUtil.getValue("BUSINESS_MONTH")),
				forecastDataFrame.col(TrendingModelUtil
						.getValue("LAUNCH_MONTH_IND")), forecastDataFrame
						.col(TrendingModelUtil.getValue("MODEL_YEAR"))));

		// Reading the major and minor change coefficients values.
		if (null != values) {
			majorChangeCoefficient = values.containsKey(TrendingModelUtil
					.getValue("MAJOR_CHANGE")) ? values.get(TrendingModelUtil
					.getValue("MAJOR_CHANGE"))
					: values.containsKey(TrendingModelUtil
							.getValue("MINOR_CHANGE")) ? values
							.get(TrendingModelUtil.getValue("MINOR_CHANGE"))
							: values.containsKey(TrendingModelUtil
									.getValue("REGULAR_CHANGE")) ? values
									.get(TrendingModelUtil
											.getValue("REGULAR_CHANGE")) : 0.0;
			minorChangeCoefficient = values.containsKey(TrendingModelUtil
					.getValue("MINOR_CHANGE")) ? values.get(TrendingModelUtil
					.getValue("MINOR_CHANGE"))
					: values.containsKey(TrendingModelUtil
							.getValue("MAJOR_CHANGE")) ? values
							.get(TrendingModelUtil.getValue("MAJOR_CHANGE"))
							: values.containsKey(TrendingModelUtil
									.getValue("REGULAR_CHANGE")) ? values
									.get(TrendingModelUtil
											.getValue("REGULAR_CHANGE")) : 0.0;

			regularChangeCoefficient = values.containsKey(TrendingModelUtil
					.getValue("REGULAR_CHANGE")) ? values.get(TrendingModelUtil
					.getValue("REGULAR_CHANGE"))
					: values.containsKey(TrendingModelUtil
							.getValue("MAJOR_CHANGE")) ? values
							.get(TrendingModelUtil.getValue("MAJOR_CHANGE"))
							: values.containsKey(TrendingModelUtil
									.getValue("MINOR_CHANGE")) ? values
									.get(TrendingModelUtil
											.getValue("MINOR_CHANGE")) : 0.0;

			newModelYearFlagCoefficient = values.containsKey(TrendingModelUtil
					.getValue("NEW_MODEL_YEAR_FLAG")) ? values
					.get(TrendingModelUtil.getValue("NEW_MODEL_YEAR_FLAG"))
					: 0.0;
			logger.info("majorChangeCoefficient: " + majorChangeCoefficient
					+ " minorChangeCoefficient: " + minorChangeCoefficient
					+ " regularChangeCoefficient: " + regularChangeCoefficient
					+ "newModelYearFlagCoefficient: "
					+ newModelYearFlagCoefficient);
		} else {
			majorChangeCoefficient = 0.0;
			minorChangeCoefficient = 0.0;
			newModelYearFlagCoefficient = 0.0;
			regularChangeCoefficient = 0.0;
			logger.info("majorChangeCoefficient: " + majorChangeCoefficient
					+ " minorChangeCoefficient: " + minorChangeCoefficient
					+ " regularChangeCoefficient: " + regularChangeCoefficient
					+ "newModelYearFlagCoefficient: "
					+ newModelYearFlagCoefficient);
		}
		logger.info("MSRP Calculation started. ");

		// Creating the Map output.
		Map<Integer, Double> msrpAmounts = new HashMap<>();

		finalFrame.show();

		// Reading the model year changes values.
		TrendModelManager trendModelManager = new TrendModelManager();
		Map<Integer, String> modelYearChangesMap = trendModelManager
				.calculateLaunchMonthIndicator(forecastDataFrame);
		logger.info("modelYearChangesMap:" + modelYearChangesMap);

		// Collecting all the rows and do calculations.
		for (Row row : finalFrame.collect()) {

			// Checking the coefficient values.
			int majorChangeVal = row.isNullAt(5) ? 0 : row.getInt(5);
			int minorChangeVal = row.isNullAt(6) ? 0 : row.getInt(6);
			int regularChange = row.isNullAt(7) ? 0 : row.getInt(7);
			int newModelYearFlagVal = row.isNullAt(1) ? 0 : row.getInt(1);

			// Calculate Average MSRP value.
			double avgMsrpPct = intercept
					+ (majorChangeVal * majorChangeCoefficient)
					+ (minorChangeVal * minorChangeCoefficient)
					+ (regularChange * regularChangeCoefficient)
					+ (newModelYearFlagVal * newModelYearFlagCoefficient);

			// Checking the negative values.
			if (avgMsrpPct > 0) {
				avgMsrpChgPctValues.add(avgMsrpPct);
			} else {
				avgMsrpChgPctValues.add(0.0);
			}

			// Reading the business month, launch month indicator and model
			// year.
			String businessMonth = row.isNullAt(2) ? "" : row.getString(2);
			int launchMonthIndicator = row.isNullAt(3) ? 0 : row.getInt(3);
			int modelYear = row.isNullAt(4) ? 0 : row.getInt(4);
			logger.info("launchMonthIndicator: " + launchMonthIndicator
					+ " : modelYear: " + modelYear + " businessMonth: "
					+ businessMonth);

			// Get business month of particular model year.
			if (modelYearChangesMap.containsKey(modelYear)) {
				String launchMonth = modelYearChangesMap.get(modelYear);
				if (launchMonth.equalsIgnoreCase(businessMonth)) {
					double currentMonthMsrp = msrpAmounts
							.containsKey(modelYear) ? msrpAmounts
							.get(modelYear) : previousMsrpValue;
					currentMonthMsrp = currentMonthMsrp * (1 + avgMsrpPct);
					forecastMsrpValues.add((double) Math
							.round(currentMonthMsrp / 50) * 50);
					previousMsrpValue = currentMonthMsrp;
					msrpAmounts.put(modelYear, currentMonthMsrp);
				} else {
					double currentMonthMsrp = msrpAmounts
							.containsKey(modelYear) ? msrpAmounts
							.get(modelYear) : previousMsrpValue;
					forecastMsrpValues.add((double) Math
							.round(currentMonthMsrp / 50) * 50);
					previousMsrpValue = currentMonthMsrp;
					msrpAmounts.put(modelYear, currentMonthMsrp);
				}
			} else {
				double currentMonthMsrp = msrpAmounts.containsKey(modelYear) ? msrpAmounts
						.get(modelYear) : previousMsrpValue;
				forecastMsrpValues.add((double) Math
						.round(currentMonthMsrp / 50) * 50);
				previousMsrpValue = currentMonthMsrp;
				msrpAmounts.put(modelYear, currentMonthMsrp);
			}
		}

		// Displaying the average MSRP values and MSRP Amount values.
		msrpValues.put(TrendingModelUtil.getValue("AVG_MSRP_CHG_PCT"),
				avgMsrpChgPctValues);
		msrpValues.put(TrendingModelUtil.getValue("MSRP"), forecastMsrpValues);
		logger.info("avgMsrpChgPctValues: " + avgMsrpChgPctValues);
		logger.info("forecastMsrpValues: " + forecastMsrpValues);
		logger.info("MSRP Calculation ended. ");
		return msrpValues;
	}

	/***
	 * Changing the string data types into double values
	 * 
	 * @param historyDataFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame getMsrpRegressionInput(DataFrame historyDataFrame)
			throws AnalyticsRuntimeException {

		// Selecting the required columns
		// change_category_cd,avg_msrp_amt_chg_pct and new_model_year_flag.
		logger.info("Change category values calculation started.");
		DataFrame categoryFrame = historyDataFrame
				.select(historyDataFrame.col(TrendingModelUtil
						.getValue("CHANGE_CATEGORY")), historyDataFrame
						.col(TrendingModelUtil.getValue("AVG_MSRP_CHG_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")));

		// Generate the values for each category.
		JavaRDD<String> categoryValues = predictChangeCategoryValues(categoryFrame);
		logger.info("categoryValues: " + categoryValues.count());

		// Reading all the change category codes data.
		JavaRDD<ChangeCategory> monthvalueColumns = generateAllChangeCategoryValues(categoryValues);
		logger.info("Change category values calculation ended.");

		// Returning the frame with the major, minor and regular change.
		return addChangeCategoryColumnsToDataFrame(categoryFrame,
				categoryValues.collect(),
				prepareChangeCategoryRegressionInput(monthvalueColumns));
	}

	/***
	 * Changing the string data types into double values
	 * 
	 * @param msrpForecastFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame getMsrpForecastInput(DataFrame msrpForecastFrame)
			throws AnalyticsRuntimeException {

		// This statement reads the required columns from the frame.
		DataFrame categoryFrame = msrpForecastFrame.select(msrpForecastFrame
				.col(TrendingModelUtil.getValue("CHANGE_CATEGORY")),
				msrpForecastFrame.col(TrendingModelUtil
						.getValue("NEW_MODEL_YEAR_FLAG")), msrpForecastFrame
						.col(TrendingModelUtil.getValue("BUSINESS_MONTH")),
				msrpForecastFrame.col(TrendingModelUtil
						.getValue("LAUNCH_MONTH_IND")), msrpForecastFrame
						.col(TrendingModelUtil.getValue("MODEL_YEAR")));

		// Generate the values for the for each Change Category.
		JavaRDD<String> categoryValues = predictChangeCategoryValues(categoryFrame);
		logger.info("categoryValues: " + categoryValues.count());

		// Reading all the change category columns data.
		JavaRDD<ChangeCategory> changeCategoryValueColumns = generateAllChangeCategoryValues(categoryValues);

		// Returning the frame with the major, minor and regular change.
		return addChangeCategoryColumnsToDataFrame(
				categoryFrame,
				categoryValues.collect(),
				prepareChangeCategoryRegressionInput(changeCategoryValueColumns));
	}

	/**
	 * This method returns the change category column values.
	 * 
	 * @param inputFrame
	 * @return
	 */
	private JavaRDD<String> predictChangeCategoryValues(DataFrame inputFrame) {
		return inputFrame.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 2245714652058215812L;

			@Override
			public String call(Row row) throws Exception {
				return row.getString(0);
			}
		});
	}

	/**
	 * This reads the change category and returns with major , minor and regular
	 * change data.
	 * 
	 * @param months
	 * @return
	 */
	private JavaRDD<ChangeCategory> generateAllChangeCategoryValues(
			JavaRDD<String> months) {
		return months.map(new Function<String, ChangeCategory>() {
			private static final long serialVersionUID = 5915097829446352443L;

			@Override
			public ChangeCategory call(String key) throws Exception {
				return getChangeCategoryColumnValues(key);
			}
		});
	}

	/**
	 * This methods generates the column values based on the column type.
	 * 
	 * @param key
	 * @return
	 */
	private ChangeCategory getChangeCategoryColumnValues(String key) {
		ChangeCategory changeCategory = null;

		if (key.equalsIgnoreCase("FMC") || key.equalsIgnoreCase("MMC")) {
			changeCategory = generateChangeCategoryColumnValues("1,0,0");
		} else if (key.equalsIgnoreCase("MC")) {
			changeCategory = generateChangeCategoryColumnValues("0,1,0");
		} else {
			changeCategory = generateChangeCategoryColumnValues("0,0,1");
		}
		return changeCategory;
	}

	/**
	 * This method sets the values to change category instance.
	 * 
	 * @param monthValues
	 * @return
	 */
	private ChangeCategory generateChangeCategoryColumnValues(String monthValues) {
		ChangeCategory changeCategory = new ChangeCategory();
		String[] values = StringUtils.split(monthValues, ",");
		changeCategory.setMajorChange(values[0]);
		changeCategory.setMinorChange(values[1]);
		changeCategory.setRegularChange(values[2]);
		return changeCategory;
	}

	/**
	 * This method prepares the regression input like major and minor change.
	 * 
	 * @param monthvalueColumns
	 * @return
	 */
	private Map<String, List<String>> prepareChangeCategoryRegressionInput(
			JavaRDD<ChangeCategory> monthvalueColumns) {

		// All the columns data.
		Map<String, List<String>> changeCategoryMap = new LinkedHashMap<>();
		List<String> majorChangeList = new ArrayList<>();
		List<String> minorChangeList = new ArrayList<>();
		List<String> regularChangeList = new ArrayList<>();

		// Collecting the data.
		for (ChangeCategory changeDTO : monthvalueColumns.collect()) {
			majorChangeList.add(changeDTO.getMajorChange());
			minorChangeList.add(changeDTO.getMinorChange());
			regularChangeList.add(changeDTO.getRegularChange());
		}

		// Adding to map.
		changeCategoryMap.put("major_change", majorChangeList);
		changeCategoryMap.put("minor_change", minorChangeList);
		changeCategoryMap.put("regular_change", regularChangeList);
		return changeCategoryMap;
	}

	/**
	 * This method adds the change category columns to data frame.
	 * 
	 * @param historyDataFrame
	 * @param monthValues
	 * @param mapMonths
	 * @return
	 */
	private DataFrame addChangeCategoryColumnsToDataFrame(
			DataFrame historyDataFrame, List<String> monthValues,
			Map<String, List<String>> mapMonths) {
		List<Row> rows = historyDataFrame.collectAsList();
		List<Row> rowsWithForecastValues = new ArrayList<>(rows.size());

		// Reading the forecast values.
		for (int j = 0; j < rows.size(); j++) {
			List<Object> newRow = new ArrayList<>(
					JavaConversions.asJavaList(rows.get(j).toSeq()));
			for (Map.Entry<String, List<String>> entry : mapMonths.entrySet()) {
				List<String> listValues = entry.getValue();
				newRow.add(Integer.parseInt(listValues.get(j)));
			}
			newRow.add(monthValues.get(j));
			rowsWithForecastValues.add(RowFactory.create(newRow
					.toArray(new Object[newRow.size()])));
		}

		// Adding values to input frame.
		List<StructField> structFields = new ArrayList<>(
				JavaConversions.asJavaList(historyDataFrame.schema()));
		for (Map.Entry<String, List<String>> entry : mapMonths.entrySet()) {
			String columnName = entry.getKey();
			structFields.add(new StructField(columnName, DataTypes.IntegerType,
					true, Metadata.empty()));
		}
		StructType structType = new StructType(
				structFields.toArray(new StructField[structFields.size()]));
		historyDataFrame = TrendingModelUtil.getHivecontext().createDataFrame(
				TrendingModelUtil.getJavaSparkContext().parallelize(
						rowsWithForecastValues), structType);
		return historyDataFrame;
	}
}
