/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.regression.LinearModelDTO;
import com.toyota.analytics.common.util.TrendingModelUtil;

/**
 * This class contains the methods which will do incentive trending
 * calculations.Here, will do regression on the input series data set based on
 * the dependent variable (tms_inctv_pct,increasing_tms_inctv_pct,pin_inctv_pct,
 * increasing_pin_inctv_pct) and independent variables list
 * (years_since_major_change,months_after_launch_nbr,months_b4_next_launch) and
 * get the intercept and coefficient values for variables list. After we got the
 * intercept and coefficient values will predict the forecast values for
 * incentives.
 * 
 * @author Naresh
 *
 */
public class IncentiveTrendModelManager implements Serializable {

	private static final long serialVersionUID = 2822513732859894996L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(IncentiveTrendModelManager.class);

	/**
	 * This method will do incentive trending calculations.Here, we will get the
	 * intercept and coefficient values for variables list. After we got the
	 * intercept and coefficient values will predict the forecast values for
	 * incentives.
	 * 
	 * @param dependentVariable
	 * @param futureFrame
	 * @param linearModelDTO
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public List<Double> predictIncentiveForecastValues(
			String dependentVariable, DataFrame futureFrame,
			LinearModelDTO linearModelDTO, double previousIncentiveValue,
			DataFrame historyDataFrame) throws IOException,
			AnalyticsRuntimeException {
		logger.info("previousIncentiveValue " + previousIncentiveValue);
		// Reading the column coefficients list and intercept.
		List<Double> predictList = new ArrayList<>();
		Map<String, Double> values = linearModelDTO.getCoefficientValues();
		final double intercept = linearModelDTO.getIntercept();
		final double yearsSinceMajorChangeCoefficient;
		final double monthsAfterLaunchCoefficient;
		final double monthsBeforeNextLaunchCoefficient;

		// Reading the years_since_major_change coefficient value.
		if (null != values) {
			yearsSinceMajorChangeCoefficient = values
					.containsKey(TrendingModelUtil
							.getValue("YEARS_SINCE_MAJOR_CHANGE")) ? values
					.get(TrendingModelUtil.getValue("YEARS_SINCE_MAJOR_CHANGE"))
					: 0.0;
			monthsAfterLaunchCoefficient = values.containsKey(TrendingModelUtil
					.getValue("MONTHS_AFTER_LAUNCH")) ? values
					.get(TrendingModelUtil.getValue("MONTHS_AFTER_LAUNCH"))
					: 0.0;
			monthsBeforeNextLaunchCoefficient = values
					.containsKey(TrendingModelUtil
							.getValue("MONTHS_B4_NEXT_LAUNCH")) ? values
					.get(TrendingModelUtil.getValue("MONTHS_B4_NEXT_LAUNCH"))
					: 0.0;
		} else {
			yearsSinceMajorChangeCoefficient = 0.0;
			monthsAfterLaunchCoefficient = 0.0;
			monthsBeforeNextLaunchCoefficient = 0.0;
		}

		DataFrame pctFrame = futureFrame.select(futureFrame
				.col(TrendingModelUtil.getValue("YEARS_SINCE_MAJOR_CHANGE")),
				futureFrame.col(TrendingModelUtil
						.getValue("MONTHS_AFTER_LAUNCH")), futureFrame
						.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")));

		// Collecting all the rows and do calculations.
		// Reading the configuration property.
		final double confProperty = Integer.parseInt(TrendingModelUtil
				.getValue("CONF_PROP"));

		// Reading the Min, Max and Avg for the Incentives.
		Map<Integer, IncentiveDetails> incentiveDetails = null;
		if (null != dependentVariable
				&& dependentVariable.equals(TrendingModelUtil
						.getValue("INCENTIVE_PCT"))) {
			incentiveDetails = getMinMaxAvgForInctvPct(historyDataFrame);
		} else {
			incentiveDetails = getMinMaxAvgForInctvPinPct(historyDataFrame);
		}

		for (Row row : pctFrame.collect()) {
			double predictedIncentive;
			// Checking null values.
			int yearsSinceMajorChangeVal = row.isNullAt(0) ? 0 : row.getInt(0);
			int monthsAfterLaunchVal = row.isNullAt(1) ? 0 : row.getInt(1);
			int monthsBeforeNextLaunchVal = row.isNullAt(2) ? 0 : row.getInt(2);

			if (yearsSinceMajorChangeVal > confProperty) {
				predictedIncentive = intercept
						+ (confProperty * yearsSinceMajorChangeCoefficient)
						+ (monthsAfterLaunchVal * monthsAfterLaunchCoefficient)
						+ (monthsBeforeNextLaunchVal * monthsBeforeNextLaunchCoefficient);
			} else {
				predictedIncentive = intercept
						+ (yearsSinceMajorChangeVal * yearsSinceMajorChangeCoefficient)
						+ (monthsAfterLaunchVal * monthsAfterLaunchCoefficient)
						+ (monthsBeforeNextLaunchVal * monthsBeforeNextLaunchCoefficient);
			}

			// Checking the accurate value.
			if (null != incentiveDetails && !incentiveDetails.isEmpty()) {
				IncentiveDetails incentivesVals = incentiveDetails
						.containsKey(yearsSinceMajorChangeVal) ? incentiveDetails
						.get(yearsSinceMajorChangeVal) : null;
				if (null != incentivesVals) {
					if (predictedIncentive < incentivesVals
							.getMinIncentivePct()
							&& predictedIncentive > incentivesVals
									.getMaxIncentivePct()) {
						predictList.add(incentivesVals.getAvgIncentivePct());

					} else {
						predictList.add(predictedIncentive);
					}
				}
			}

			predictList.add(predictedIncentive);
		}

		// Checking the values.
		List<Double> defaultList = new ArrayList<>();
		if (checkColumnsData(predictList)) {
			logger.info("Dependent Variable: " + dependentVariable
					+ " forecastIncentiveValues: " + predictList);
			return predictList;
		} else {
			for (int counter = 0; counter < predictList.size(); counter++) {
				defaultList.add(previousIncentiveValue);
			}
			logger.info("Dependent Variable: " + dependentVariable
					+ " defaultList: " + defaultList);
			return defaultList;
		}
	}

	/**
	 * This method runs the incentive model and gives the forecast values.
	 * 
	 * @param trendInputFrame
	 * @param dependentVariable
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public List<Double> predictIncentiveOutput(DataFrame trendInputFrame,
			String dependentVariable) throws IOException,
			AnalyticsRuntimeException {

		// Initializing incentive trend model manager.
		List<Double> forecastValues = new ArrayList<>();

		DataFrame requiredColumnsFrame = trendInputFrame.select(
				trendInputFrame.col(dependentVariable),
				trendInputFrame.col(TrendingModelUtil.getValue("MSRP")));

		// Reading independent columns list.
		for (Row row : requiredColumnsFrame.collect()) {
			// Null checks added column values.
			double dependentVar = row.isNullAt(0) ? 0.0 : row.getDouble(0);
			double msrp = row.isNullAt(1) ? 0.0 : row.getDouble(1);

			// Calculates the value in dollars.
			double forecastValue = msrp * dependentVar;
			forecastValues.add((double) Math.round(forecastValue / 50) * 50);
		}
		return forecastValues;
	}

	/**
	 * 
	 * @param trendInputFrame
	 * @param dependentVariable
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public double collectPreviousInctvPct(DataFrame trendInputFrame,
			String dependentVariable) throws IOException,
			AnalyticsRuntimeException {

		// Initializing incentive trend model manager.
		List<Double> incentivePctValues = new ArrayList<>();

		DataFrame requiredColumnsFrame = trendInputFrame.select(trendInputFrame
				.col(dependentVariable));

		// Reading independent columns list.
		for (Row row : requiredColumnsFrame.collect()) {
			// Null checks added column values.
			double dependentVar = row.isNullAt(0) ? 0.0 : row.getDouble(0);
			if (dependentVar != 0.0) {
				incentivePctValues.add(dependentVar);
			}
		}

		// Read the last value from list.
		double previousInctvPct;
		if (!incentivePctValues.isEmpty()) {
			previousInctvPct = incentivePctValues
					.get(incentivePctValues.size() - 1);
		} else {
			previousInctvPct = 0.0;
		}
		return previousInctvPct;
	}

	public static boolean checkColumnsData(List<Double> incentivesData) {

		boolean status = false;
		Set<Double> listOfIncentives = new HashSet<>();
		if (null != incentivesData && !incentivesData.isEmpty()) {
			for (Double inctvPct : incentivesData) {
				listOfIncentives.add(inctvPct);
			}
		}
		// Checking the size of the set
		if (listOfIncentives.size() > 1) {
			status = true;
		}
		return status;
	}

	/**
	 * This method calculates the increasing the incentive percent values for
	 * the incentives.
	 * 
	 * @param trendInputFrame
	 * @param dependentVariable
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public List<Double> calculateIncreasingInctvValues(
			DataFrame trendInputFrame, String dependentVariable)
			throws IOException, AnalyticsRuntimeException {

		// Initializing incentive trend model manager.
		List<Double> increasingPctList = new ArrayList<>();
		double previousIncreasingValue = 0.0;

		if (null != trendInputFrame && trendInputFrame.count() > 0) {
			DataFrame requiredColumnsFrame = trendInputFrame
					.select(trendInputFrame.col(dependentVariable));

			// Reading independent columns list.
			for (Row row : requiredColumnsFrame.collect()) {
				// Null checks added column values.
				double incentivePct = row.isNullAt(0) ? 0 : row.getDouble(0);
				if (previousIncreasingValue == 0.0) {
					previousIncreasingValue = incentivePct;
					increasingPctList.add(previousIncreasingValue);
				} else if (incentivePct > previousIncreasingValue) {
					increasingPctList.add(incentivePct);
				} else {
					increasingPctList.add(previousIncreasingValue);
				}
			}
		}
		logger.info("Dependent Variable: " + dependentVariable
				+ " increasingPctList: " + increasingPctList);
		return increasingPctList;
	}

	/**
	 * 
	 * @param historyInputFrame
	 * @return
	 */
	public Map<Integer, IncentiveDetails> getMinMaxAvgForInctvPct(
			DataFrame historyInputFrame) {

		logger.info("getMinMaxAvgForInctvPct start.");
		Map<Integer, IncentiveDetails> yearsSinceMajorChange = new HashMap<>();
		IncentiveDetails incentiveDetails = new IncentiveDetails();

		// Reading the model year and latest model year.
		DataFrame tmsIncentiveFrame = historyInputFrame
				.filter(historyInputFrame.col(
						TrendingModelUtil.getValue("INCENTIVE_PCT"))
						.$greater(0))
				.groupBy(
						historyInputFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MAJOR_CHANGE")))
				.agg(historyInputFrame.col(TrendingModelUtil
						.getValue("YEARS_SINCE_MAJOR_CHANGE")),
						max(historyInputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT"))),
						min(historyInputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT"))),
						avg(historyInputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT"))));
		tmsIncentiveFrame.show();

		// Collecting the model_year list by change category filter.
		for (Row row : tmsIncentiveFrame.collect()) {
			int yearsSinceMajorChangeVal = row.isNullAt(0) ? 0 : row.getInt(0);
			double maxInctvPct = row.isNullAt(1) ? 0.0 : row.getDouble(1);
			double minInctvPct = row.isNullAt(2) ? 0.0 : row.getDouble(2);
			double avgInctvPct = row.isNullAt(3) ? 0.0 : row.getDouble(3);

			incentiveDetails.setMinIncentivePct(minInctvPct);
			incentiveDetails.setMaxIncentivePct(maxInctvPct);
			incentiveDetails.setAvgIncentivePct(avgInctvPct);

			yearsSinceMajorChange.put(yearsSinceMajorChangeVal,
					incentiveDetails);

			logger.info("yearsSinceMajorChangeVal: " + yearsSinceMajorChangeVal
					+ " maxInctvPct: " + maxInctvPct + " : minInctvPct: "
					+ minInctvPct + " : avgInctvPct: " + avgInctvPct);
			logger.info("getMinMaxAvgForInctvPct end.");
		}
		return yearsSinceMajorChange;
	}

	/**
	 * This method will give minimum, maximum and average values of these
	 * percentages.
	 * 
	 * @param historyInputFrame
	 * @return
	 */
	public Map<Integer, IncentiveDetails> getMinMaxAvgForInctvPinPct(
			DataFrame historyInputFrame) {

		logger.info("getMinMaxAvgForInctvPinPct start.");
		Map<Integer, IncentiveDetails> yearsSinceMajorChange = new HashMap<>();
		IncentiveDetails incentiveDetails = new IncentiveDetails();

		// Reading the model year and latest model year.
		DataFrame tmsIncentivePinFrame = historyInputFrame
				.filter(historyInputFrame.col(
						TrendingModelUtil.getValue("INCENTIVE_PCT_PIN"))
						.$greater(0))
				.groupBy(
						historyInputFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MAJOR_CHANGE")))
				.agg(historyInputFrame.col(TrendingModelUtil
						.getValue("YEARS_SINCE_MAJOR_CHANGE")),
						max(historyInputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN"))),
						min(historyInputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN"))),
						avg(historyInputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN"))));
		tmsIncentivePinFrame.show();

		// Collecting the model_year list by change category filter.
		for (Row row : tmsIncentivePinFrame.collect()) {
			int yearsSinceMajorChangeVal = row.isNullAt(0) ? 0 : row.getInt(0);
			double maxInctvPinPct = row.isNullAt(1) ? 0.0 : row.getDouble(1);
			double minInctvPinPct = row.isNullAt(2) ? 0.0 : row.getDouble(2);
			double avgInctvPinPct = row.isNullAt(3) ? 0.0 : row.getDouble(3);

			incentiveDetails.setMinIncentivePct(minInctvPinPct);
			incentiveDetails.setMaxIncentivePct(maxInctvPinPct);
			incentiveDetails.setAvgIncentivePct(avgInctvPinPct);

			yearsSinceMajorChange.put(yearsSinceMajorChangeVal,
					incentiveDetails);

			logger.info("maxInctvPct: " + maxInctvPinPct + " : minInctvPct: "
					+ minInctvPinPct + " : avgInctvPct: " + avgInctvPinPct);
			logger.info("getMinMaxAvgForInctvPct end.");
		}
		return yearsSinceMajorChange;
	}
}
