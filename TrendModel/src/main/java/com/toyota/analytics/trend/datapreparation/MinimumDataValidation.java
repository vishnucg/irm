/**
 * 
 */
package com.toyota.analytics.trend.datapreparation;

import static org.apache.spark.sql.functions.sum;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import com.toyota.analytics.common.exceptions.AnalyticsDataValidationException;
import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.util.TrendingModelUtil;

/**
 * This class contains method which will do data validation before process the
 * history data.
 * 
 * 1. History data frame should contain at least one major_change, one
 * minor_change, one regular_change.
 * 
 * 2. History data frame should contain more than one model_year.
 * 
 * 3. History data frame should contain at least one business_month having
 * launch_month_ind one.
 * 
 * 4. Make sure the sum of MODEL_YEAR_SALEs_PCT for a business month is 1 when
 * summed across all the rows for a month.
 * 
 * @author Naresh
 *
 */
public class MinimumDataValidation implements Serializable {

	private static final long serialVersionUID = -871320513085371276L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(MinimumDataValidation.class);

	/**
	 * This method checks the minimum data validation on the input frame before
	 * doing the forecasting.
	 * 
	 * @param historyDataFrame
	 * @throws AnalyticsRuntimeException
	 */
	public static void runMinimumDataValidation(DataFrame historyDataFrame)
			throws AnalyticsDataValidationException {

		// Checking model year rule.
		if (!checkModelYearRule(historyDataFrame)) {
			logger.error(TrendingModelUtil.getValue("MODEL_YEAR_RULE_MSG"));
			throw new AnalyticsDataValidationException(
					TrendingModelUtil.getValue("MODEL_YEAR_RULE_MSG"));
		}
	}

	/**
	 * This method checks whether input frame contains at lease one Major, Minor
	 * or Regular in change category column.
	 * 
	 * @param inputFrame
	 * @return
	 */
	public static boolean checkMajorMinorRegularRule(DataFrame inputFrame) {
		logger.info("checkMajorMinorRegularRule start.");

		// Reading the change category from input Frame.
		DataFrame changeCategoryFrame = inputFrame.select(
				inputFrame.col(TrendingModelUtil.getValue("CHANGE_CATEGORY")))
				.distinct();
		long numberOfRows = changeCategoryFrame.count();
		logger.info("checkMajorMinorRegularRule end.");
		return numberOfRows > 0 ? true : false;
	}

	/**
	 * This method checks whether history frame contains more than one model
	 * year or not.
	 * 
	 * @param histroyDataFrame
	 * @return
	 */
	public static boolean checkModelYearRule(DataFrame histroyDataFrame) {
		logger.info("checkModelYearRule start.");

		// Reading the change category from input Frame.
		DataFrame modelYearFrame = histroyDataFrame.select(
				histroyDataFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")))
				.distinct();
		long numberOfModelYears = modelYearFrame.count();
		logger.info("checkModelYearRule end.");
		return numberOfModelYears > 1 ? true : false;
	}

	/**
	 * This method checks whether history frame contains at least one business
	 * month should have launch month indicator is one or not.
	 * 
	 * @param histroyDataFrame
	 * @return
	 */
	public static boolean checkLaunchMonthIndicatorRule(
			DataFrame histroyDataFrame) {
		logger.info("checkLaunchMonthIndicatorRule start.");

		// Reading the business_month, launch_month_ind from input Frame.
		DataFrame launchMonthIndicatorFrame = histroyDataFrame.select(
				histroyDataFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")),
				histroyDataFrame.col(TrendingModelUtil
						.getValue("LAUNCH_MONTH_IND"))).filter(
				histroyDataFrame.col(
						TrendingModelUtil.getValue("LAUNCH_MONTH_IND"))
						.equalTo(1));
		long launchMonthIndicatorRows = launchMonthIndicatorFrame.count();
		logger.info("checkLaunchMonthIndicatorRule end.");
		return launchMonthIndicatorRows > 0 ? true : false;
	}

	/**
	 * This method checks whether history frame contains sum of the model year
	 * sales percent should be one business month or not.
	 * 
	 * @param histroyDataFrame
	 * @return
	 */
	public static boolean checkSumOfModelYearPctForBusinessMonth(
			DataFrame histroyDataFrame) {
		logger.info("checkSumOfModelYearPctForBusinessMonth start.");

		// Reading the business_month and model_year_sales_pct from input Frame.
		DataFrame sumOfModelYearPctFrame = histroyDataFrame.groupBy(
				histroyDataFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")))
				.agg(histroyDataFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")),
						sum(
								histroyDataFrame.col(TrendingModelUtil
										.getValue("MODEL_YEAR_SALES_PCT"))).as(
								TrendingModelUtil
										.getValue("MODEL_YEAR_SALES_PCT_SUM")));
		long count = sumOfModelYearPctFrame.filter(
				sumOfModelYearPctFrame.col(
						TrendingModelUtil.getValue("MODEL_YEAR_SALES_PCT_SUM"))
						.equalTo(1)).count();
		long modelYearPctValue = sumOfModelYearPctFrame.count();
		logger.info("checkSumOfModelYearPctForBusinessMonth end.");
		return count == modelYearPctValue ? true : false;
	}

	/**
	 * This method checks whether input frame contains at lease one Major change
	 * in history data frame or not.
	 * 
	 * @param inputFrame
	 * @return
	 */
	public static boolean checkMajorRule(DataFrame inputFrame) {
		logger.info("checkMajorRule start.");

		// Reading the change category from input Frame.
		DataFrame majorFrame = inputFrame.select(
				inputFrame.col(TrendingModelUtil.getValue("CHANGE_CATEGORY")))
				.distinct();

		majorFrame = majorFrame.filter(inputFrame.col(
				TrendingModelUtil.getValue("CHANGE_CATEGORY")).equalTo(
				TrendingModelUtil.getValue("MAJOR_FILTER")));
		logger.info("checkMajorRule end.");
		return majorFrame.count() > 0 ? true : false;
	}

	/**
	 * This method checks whether input frame contains at lease one Major change
	 * in history data frame or not.
	 * 
	 * @param inputFrame
	 * @return
	 */
	public static boolean checkMinorRule(DataFrame inputFrame) {
		logger.info("checkMinorRule start.");

		// Reading the change category from input Frame.
		DataFrame minorFrame = inputFrame
				.select(inputFrame.col(TrendingModelUtil
						.getValue("CHANGE_CATEGORY")))
				.distinct()
				.filter(inputFrame.col(
						TrendingModelUtil.getValue("CHANGE_CATEGORY")).equalTo(
						TrendingModelUtil.getValue("MINOR_FILTER")));
		logger.info("checkMinorRule end.");
		return minorFrame.count() > 0 ? true : false;
	}
}
