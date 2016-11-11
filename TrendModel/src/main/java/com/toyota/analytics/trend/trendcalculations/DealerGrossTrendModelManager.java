/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.regression.LinearModelDTO;
import com.toyota.analytics.common.util.TrendingModelUtil;

/**
 * This class contains the methods which will do dealer gross forecasting. Here,
 * will do regression on the history data set as per the series level and based
 * on the dependent variable (dealer_gross_pct) and independent variables list
 * (years_since_major_change,months_after_launch_nbr,months_b4_next_launch) and
 * get the intercept and coefficient values for variables list. After we got the
 * intercept and coefficient values will predict the forecast values for dealer
 * gross percentage based on the future data set.
 * 
 * @author Naresh
 *
 */
public class DealerGrossTrendModelManager implements Serializable {

	private static final long serialVersionUID = -7748545399725186425L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(DealerGrossTrendModelManager.class);

	/**
	 * Here, This method will do regression on the history data set as per the
	 * series level and based on the dependent variable (dealer_gross_pct) and
	 * independent variables list
	 * (years_since_major_change,months_after_launch_nbr,months_b4_next_launch).
	 * We can get the intercept and coefficient values for variables list. After
	 * we got the intercept and coefficient values will predict the forecast
	 * values for dealer gross percentage based on the future data set.
	 * 
	 * @param regressionInputFrame
	 * @param trendInputFrame
	 * @param dependentVar
	 * @param independentVariables
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public List<Double> calculateDealerGrossTrend(
			DataFrame regressionInputFrame, DataFrame trendInputFrame,
			String dependentVar, List<String> independentVariables)
			throws IOException, AnalyticsRuntimeException {
		List<Double> forecastDealerGrossValues = null;

		// Run the regression and get the intercept and coefficient values.
		TrendModelManager trendModelManager = new TrendModelManager();
		LinearModelDTO linearModelDTO = null;
		try {
			// Check the unity columns data.
			logger.info("Befoe check the data in frame: "
					+ independentVariables);
			independentVariables = TrendingModelUtil.checkColumnsData(
					regressionInputFrame, independentVariables);
			logger.info("After check the data in frame: "
					+ independentVariables);
			linearModelDTO = trendModelManager.callRegressionModel(
					regressionInputFrame, dependentVar, independentVariables,
					false);
		} catch (Exception exception) {
			logger.warn(exception.getMessage());
		}

		try {
			// Calculate dealer gross percentage Values.
			forecastDealerGrossValues = predictDealerGrossValues(
					trendInputFrame, linearModelDTO);
		} catch (AnalyticsRuntimeException analyticsRuntimeException) {
			logger.warn(analyticsRuntimeException.getMessage());
		}
		return forecastDealerGrossValues;
	}

	/**
	 * This method predicts the dealer gross percentage values. This method
	 * calculates the dealer gross percent by using the taking the minimum of
	 * (dealer_gross_pct * pct cap value, predicted dealer_gross_pct). Here, cap
	 * value considered 0.8 as per the requirement.
	 * 
	 * @param futureDataFrame
	 * @param linearModelInformation
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private List<Double> predictDealerGrossValues(DataFrame futureDataFrame,
			LinearModelDTO linearModelInformation) throws IOException,
			AnalyticsRuntimeException {
		List<Double> forecastDealerGrossValues = new ArrayList<>();

		// Reading the dealer gross model column coefficients list and
		// intercept.
		Map<String, Double> values = linearModelInformation
				.getCoefficientValues();
		final double intercept = linearModelInformation.getIntercept();
		final double yearsSinceMajorChangeCoefficient;
		final double monthsAfterLaunchCoefficient;
		final double monthsBeforeNextLaunchCoefficient;

		DataFrame dealerFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("YEARS_SINCE_MAJOR_CHANGE")),
				futureDataFrame.col(TrendingModelUtil
						.getValue("MONTHS_AFTER_LAUNCH")), futureDataFrame
						.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")),
				futureDataFrame.col(TrendingModelUtil
						.getValue("DEALER_MARGIN_PCT")));

		// Collecting all the rows and do calculations.
		// Reading the configuration property.
		final double confProperty = Integer.parseInt(TrendingModelUtil
				.getValue("CONF_PROP"));

		if (null != values) {
			// Reading the years_since_major_change coefficient value.
			yearsSinceMajorChangeCoefficient = values
					.containsKey(TrendingModelUtil
							.getValue("YEARS_SINCE_MAJOR_CHANGE")) ? values
					.get(TrendingModelUtil.getValue("YEARS_SINCE_MAJOR_CHANGE"))
					: 0.0;

			// Reading the years_since_major_change coefficient value.
			monthsAfterLaunchCoefficient = values.containsKey(TrendingModelUtil
					.getValue("MONTHS_AFTER_LAUNCH")) ? values
					.get(TrendingModelUtil.getValue("MONTHS_AFTER_LAUNCH"))
					: 0.0;

			// Reading the years_since_major_change coefficient value.
			monthsBeforeNextLaunchCoefficient = values
					.containsKey(TrendingModelUtil
							.getValue("MONTHS_B4_NEXT_LAUNCH")) ? values
					.get(TrendingModelUtil.getValue("MONTHS_B4_NEXT_LAUNCH"))
					: 0.0;
			logger.info("yearsSinceMajorChangeCoefficient: "
					+ yearsSinceMajorChangeCoefficient
					+ " monthsAfterLaunchCoefficient: "
					+ monthsAfterLaunchCoefficient
					+ " monthsBeforeNextLaunchCoefficient: "
					+ monthsBeforeNextLaunchCoefficient);
		} else {
			// Setting the default values to if regression fails.
			yearsSinceMajorChangeCoefficient = 0.0;
			monthsAfterLaunchCoefficient = 0.0;
			monthsBeforeNextLaunchCoefficient = 0.0;
			logger.info("yearsSinceMajorChangeCoefficient: "
					+ yearsSinceMajorChangeCoefficient
					+ " monthsAfterLaunchCoefficient: "
					+ monthsAfterLaunchCoefficient
					+ " monthsBeforeNextLaunchCoefficient: "
					+ monthsBeforeNextLaunchCoefficient);
		}

		// Reading the in_dealer_gross_cap value.
		final double inDealerGrossCapValue = Double
				.parseDouble(TrendingModelUtil.getValue("IN_DEALER_GROSS_CAP"));

		for (Row row : dealerFrame.collect()) {
			double predictedDealerGrossPercent;
			int yearsSinceMajorChangeVal = row.isNullAt(0) ? 0 : row.getInt(0);
			int monthsAfterLaunchVal = row.isNullAt(1) ? 0 : row.getInt(1);
			int monthsBeforeNextLaunchVal = row.isNullAt(2) ? 0 : row.getInt(2);

			// Reading the yearsSinceMajorChange value from the
			// row.Calculating the forecast dealer gross values.
			if (yearsSinceMajorChangeVal > confProperty) {
				predictedDealerGrossPercent = intercept
						+ (confProperty * yearsSinceMajorChangeCoefficient)
						+ (monthsAfterLaunchVal * monthsAfterLaunchCoefficient)
						+ (monthsBeforeNextLaunchVal * monthsBeforeNextLaunchCoefficient);
			} else {
				predictedDealerGrossPercent = intercept
						+ (yearsSinceMajorChangeVal * yearsSinceMajorChangeCoefficient)
						+ (monthsAfterLaunchVal * monthsAfterLaunchCoefficient)
						+ (monthsBeforeNextLaunchVal * monthsBeforeNextLaunchCoefficient);
			}

			// Calculating the dealer gross value cap.
			double dealerMarginPercent = row.isNullAt(3) ? 0.0 : row
					.getDouble(3);
			double dealerGrossPercentCap = dealerMarginPercent
					* inDealerGrossCapValue;
			forecastDealerGrossValues.add(Math.min(dealerGrossPercentCap,
					predictedDealerGrossPercent));
		}
		logger.info("forecastDealerGrossValues: " + forecastDealerGrossValues);
		return forecastDealerGrossValues;
	}
}
