/**
 * 
 */
package com.toyota.analytics.trend.datapreparation;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.util.TrendingModelUtil;
import com.toyota.analytics.trend.trendcalculations.TrendCalculationsDTO;
import com.toyota.analytics.trend.trendcalculations.TrendModelInputDetails;
import com.toyota.analytics.trend.trendcalculations.TrendModelManager;

/**
 * This class contains methods which can connect to hive tables using hive
 * context and will prepare the data as per requirement. This class prepares the
 * history input and future input based on the input queries.
 * 
 * This contains the methods which will calculates the average MSRP and average
 * MSRP change percent over the previous year on history data frame.
 * 
 * And also contains the methods which will calculates the required columns and
 * will add to history data frame and future data frame.
 * 
 * @author Naresh
 *
 */
public class TrendDataPreparation implements Serializable {

	private static final long serialVersionUID = 5995868500054255932L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(TrendDataPreparation.class);

	/**
	 * This method reads the history input data using hive query from hive table
	 * and prepare the required data.
	 * 
	 * @param trendingModelInfo
	 * @return
	 * @throws TrendModelBaseException
	 */
	public DataFrame getTrendModelsInput(TrendCalculationsDTO trendingModelInfo)
			throws AnalyticsRuntimeException {
		logger.info("Reading trend regression input started.");
		// Get hive connection object using hive context and uses the trending
		// database.
		HiveContext hiveContext = trendingModelInfo.getHiveContext();

		// Returns the history input.
		return hiveContext.sql(TrendingModelUtil
				.getValue("TREND_MODEL_INPUT_QUERY"));
	}

	/**
	 * This method calculates the average MSRP based on the series_id and
	 * model_year and change_category. We will calculates this average on the
	 * history data frame
	 * 
	 * @param historyDataFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame calculateAvgMSRP(DataFrame historyDataFrame)
			throws AnalyticsRuntimeException {

		// Filtering the frame with MSRP.
		historyDataFrame = historyDataFrame.filter(historyDataFrame.col(
				TrendingModelUtil.getValue("MSRP")).$greater(0));
		historyDataFrame.show();

		// Taking the average of MSRP group by series id and model
		// year.
		return historyDataFrame.groupBy(
				TrendingModelUtil.getValue("SERIES_ID"),
				TrendingModelUtil.getValue("MODEL_YEAR"),
				TrendingModelUtil.getValue("CHANGE_CATEGORY"),
				TrendingModelUtil.getValue("NEW_MODEL_YEAR_FLAG")).agg(
				historyDataFrame.col(TrendingModelUtil.getValue("SERIES_ID")),
				historyDataFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")),
				avg(historyDataFrame.col(TrendingModelUtil.getValue("MSRP")))
						.as(TrendingModelUtil.getValue("AVG_MSRP")),
				historyDataFrame.col(TrendingModelUtil
						.getValue("CHANGE_CATEGORY")),
				historyDataFrame.col(TrendingModelUtil
						.getValue("NEW_MODEL_YEAR_FLAG")));
	}

	/**
	 * This method calculates the MSRP Percent over previous year by using the
	 * average MSRP Frame. We will read the data and will calculate MSRP Percent
	 * over previous year by series_id and model_year.
	 * 
	 * @param firstFrame
	 * @param secondFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame getMsrpPctOverPreviousYear(DataFrame avgFrame)
			throws AnalyticsRuntimeException {

		logger.info("Calculating the MsrpPctOverPreviousYear Value started.");
		// Read required columns from first frame.
		DataFrame firstFrameData = avgFrame.select(
				avgFrame.col(TrendingModelUtil.getValue("SERIES_ID")).as(
						TrendingModelUtil.getValue("CUR_SERIES_ID")),
				avgFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")).as(
						TrendingModelUtil.getValue("CUR_MODEL_YEAR")),
				avgFrame.col(TrendingModelUtil.getValue("AVG_MSRP")).as(
						TrendingModelUtil.getValue("CUR_MSRP")),
				avgFrame.col(TrendingModelUtil.getValue("CHANGE_CATEGORY")).as(
						TrendingModelUtil.getValue("CHANGE_CATEGORY")),
				avgFrame.col(TrendingModelUtil.getValue("NEW_MODEL_YEAR_FLAG"))
						.as(TrendingModelUtil.getValue("NEW_MODEL_YEAR_FLAG")));

		// Read required columns from the second frame.
		DataFrame secondFrameData = avgFrame.select(
				avgFrame.col(TrendingModelUtil.getValue("SERIES_ID")).as(
						TrendingModelUtil.getValue("PREV_SERIES_ID")),
				avgFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")).as(
						TrendingModelUtil.getValue("PREV_MODEL_YEAR")),
				avgFrame.col(TrendingModelUtil.getValue("AVG_MSRP")).as(
						TrendingModelUtil.getValue("PREV_MSRP")));

		// Inner join with trend temporary table based on the series id, model
		// year
		// and run id.
		DataFrame innerJoinTrendFrame = firstFrameData
				.join(secondFrameData,
						firstFrameData
								.col(TrendingModelUtil
										.getValue("CUR_SERIES_ID"))
								.equalTo(
										secondFrameData.col(TrendingModelUtil
												.getValue("PREV_SERIES_ID")))
								.and(firstFrameData
										.col(TrendingModelUtil
												.getValue("CUR_MODEL_YEAR"))
										.equalTo(
												secondFrameData
														.col(TrendingModelUtil
																.getValue("PREV_MODEL_YEAR"))
														.$plus(1))),
						TrendingModelUtil.getValue("INNER_JOIN"));

		// Taking the average MSRP change of percent group by run_id, series_id
		// and change_category.
		return innerJoinTrendFrame
				.select(innerJoinTrendFrame
						.col(TrendingModelUtil.getValue("CUR_MSRP"))
						.minus(innerJoinTrendFrame.col(TrendingModelUtil
								.getValue("PREV_MSRP")))
						.divide(innerJoinTrendFrame.col(TrendingModelUtil
								.getValue("PREV_MSRP")))
						.as(TrendingModelUtil.getValue("AVG_MSRP_CHG_PCT")),
						innerJoinTrendFrame.col(TrendingModelUtil
								.getValue("CHANGE_CATEGORY")),
						innerJoinTrendFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")));
	}

	/**
	 * This method reads the each series data from history and take the required
	 * information like max business month, max model year, previous MSRP value
	 * and DPH Value.
	 * 
	 * @param historyDataFrame
	 * @param series
	 * @return
	 */
	public TrendModelInputDetails prepareProcessRequiredSeriesData(
			DataFrame seriesData) {
		logger.info("Calculating prepareProcessRequiredSeriesData started.");
		TrendModelInputDetails trendModelInputDetails = new TrendModelInputDetails();
		trendModelInputDetails.setSeriesFrame(seriesData);

		// Grouping the data by series and taking the maximum business
		// month
		DataFrame maxBusinessMonthFrame = seriesData.groupBy(
				TrendingModelUtil.getValue("SERIES")).agg(
				max(TrendingModelUtil.getValue("BUSINESS_MONTH")).as(
						TrendingModelUtil.getValue("MAX_BUSINESS_MONTH")));
		maxBusinessMonthFrame.show();

		// Checking the rows count in the frame.
		if (maxBusinessMonthFrame.count() > 0) {
			Row maxBusinessMonthRow = maxBusinessMonthFrame.first();

			// Checking the max business month null checks.
			String maxBusinessMonth = maxBusinessMonthRow.isNullAt(0) ? ""
					: maxBusinessMonthRow.getString(0);
			trendModelInputDetails.setMaxBusinessMonth(maxBusinessMonth);
			logger.info("maxBusinessMonth: " + maxBusinessMonth);

			// Taking the max model year for the max business month.
			Row maxModelYearRow = seriesData
					.filter(seriesData.col(
							TrendingModelUtil.getValue("BUSINESS_MONTH"))
							.equalTo(
									trendModelInputDetails
											.getMaxBusinessMonth()))
					.groupBy(TrendingModelUtil.getValue("BUSINESS_MONTH"))
					.agg(max(TrendingModelUtil.getValue("MODEL_YEAR")).as(
							TrendingModelUtil.getValue("MAX_MODEL_YEAR")))
					.first();
			int maxModelYear = maxModelYearRow.isNullAt(0) ? 0
					: maxModelYearRow.getInt(0);
			trendModelInputDetails.setMaxModelYear(maxModelYear);
			logger.info("maxModelYear: " + maxModelYear);

			// Taking the max model year for the max business month.
			Row minModelYearRow = seriesData
					.filter(seriesData.col(
							TrendingModelUtil.getValue("BUSINESS_MONTH"))
							.equalTo(
									trendModelInputDetails
											.getMaxBusinessMonth()))
					.groupBy(TrendingModelUtil.getValue("BUSINESS_MONTH"))
					.agg(min(TrendingModelUtil.getValue("MODEL_YEAR")).as(
							TrendingModelUtil.getValue("MAX_MODEL_YEAR")))
					.first();
			int minModelYear = minModelYearRow.isNullAt(0) ? 0
					: minModelYearRow.getInt(0);
			trendModelInputDetails.setMinModelYear(minModelYear);
			logger.info("minModelYear: " + minModelYear);

			// Taking the previous MSRP Value as per the max business month and
			// max model year.
			Row rowValue = seriesData
					.filter(seriesData
							.col(TrendingModelUtil.getValue("BUSINESS_MONTH"))
							.equalTo(
									trendModelInputDetails
											.getMaxBusinessMonth())
							.and(seriesData.col(
									TrendingModelUtil.getValue("MODEL_YEAR"))
									.equalTo(
											trendModelInputDetails
													.getMaxModelYear())))
					.select(seriesData.col(TrendingModelUtil.getValue("MSRP")),
							seriesData.col(TrendingModelUtil.getValue("DPH")))
					.first();

			// Taking the previous MSRP Value as per the max business month and
			// max model year.
			Row minRowValue = seriesData
					.filter(seriesData
							.col(TrendingModelUtil.getValue("BUSINESS_MONTH"))
							.equalTo(
									trendModelInputDetails
											.getMaxBusinessMonth())
							.and(seriesData.col(
									TrendingModelUtil.getValue("MODEL_YEAR"))
									.equalTo(
											trendModelInputDetails
													.getMinModelYear())))
					.select(seriesData.col(TrendingModelUtil.getValue("MSRP")),
							seriesData.col(TrendingModelUtil.getValue("DPH")))
					.first();

			// Added null checks and setting to input details object.
			double previousMsrpValueMinYear = minRowValue.isNullAt(0) ? 0.0
					: minRowValue.getDouble(0);
			double previousMsrpValueMaxYear = rowValue.isNullAt(0) ? 0.0
					: rowValue.getDouble(0);
			logger.info("previousMsrpValueMinYear: " + previousMsrpValueMinYear
					+ " previousMsrpValueMaxYear: " + previousMsrpValueMaxYear);
			if (previousMsrpValueMaxYear > 0) {
				trendModelInputDetails
						.setPreviousMsrpValue(previousMsrpValueMaxYear);
			} else {
				trendModelInputDetails
						.setPreviousMsrpValue(previousMsrpValueMinYear);
			}

			// Setting up the DPH Values.
			double previousDphValueMinYear = minRowValue.isNullAt(1) ? 0.0
					: minRowValue.getDouble(1);
			double previousDphValueMaxYear = rowValue.isNullAt(1) ? 0.0
					: rowValue.getDouble(1);
			logger.info("previousDphValueMinYear: " + previousDphValueMinYear
					+ " previousDphValueMaxYear: " + previousDphValueMaxYear);
			if (previousDphValueMaxYear > 0) {
				trendModelInputDetails.setDphValue(previousDphValueMaxYear);
			} else {
				trendModelInputDetails.setDphValue(previousDphValueMinYear);
			}
			logger.info("Calculating prepareProcessRequiredSeriesData ended.");
		}
		return trendModelInputDetails;
	}

	/**
	 * This method calculates the years_since_major_change,
	 * years_since_minor_change,months_after_launch, months_b4_next_launch and
	 * latest_model_year values , adds to future frame for doing the forecasting
	 * values.
	 * 
	 * @param futureFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame addRequiredOutputColumnsToFutureFrame(DataFrame futureFrame)
			throws AnalyticsRuntimeException {
		logger.info("addRequiredColumnsToInputFrames started.");
		TrendModelManager trendModelManager = new TrendModelManager();
		futureFrame = trendModelManager.addOutputColumnsToDataFrame(futureFrame
				.collectAsList(), futureFrame, trendModelManager
				.addColumnValuesToFrame(futureFrame).entrySet());
		logger.info("addRequiredColumnsToInputFrames ended.");
		return futureFrame;
	}

	/**
	 * This method calculates the sell down indicator and adds to the future
	 * frame.
	 * 
	 * @param futureFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame addSellDownIndToFutureFrame(DataFrame futureFrame)
			throws AnalyticsRuntimeException {
		logger.info("addSellDownIndToInputFrames started.");
		TrendModelManager trendModelManager = new TrendModelManager();
		futureFrame = trendModelManager.addOutputColumnsToDataFrame(futureFrame
				.collectAsList(), futureFrame, trendModelManager
				.addSellDownIndColumnToFrame(futureFrame).entrySet());
		logger.info("addSellDownIndToInputFrames ended.");
		return futureFrame;
	}

	/**
	 * This method calculates the new model year and will add the result to
	 * future frame for future calculations.
	 * 
	 * @param futureFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame addNewModelYearFlagToFutureFrame(DataFrame futureFrame)
			throws AnalyticsRuntimeException {
		logger.info("addNewModelYearFlagToFutureFrame started.");
		TrendModelManager trendModelManager = new TrendModelManager();
		futureFrame = trendModelManager.addOutputColumnsToDataFrame(futureFrame
				.collectAsList(), futureFrame, trendModelManager
				.addNewModelYearFlagToFutureFrame(futureFrame).entrySet());
		logger.info("addNewModelYearFlagToFutureFrame ended.");
		return futureFrame;
	}

	/**
	 * This method calculates the years_since_major_change,
	 * years_since_minor_change,months_after_launch, months_b4_next_launch,
	 * latest_model_year, sell_down_ind and new_model_year_flag ,adds to future
	 * frame for doing the forecasting values.
	 * 
	 * @param trendModelInputFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame addOutputColumnsToFutureFrame(
			DataFrame trendModelInputFrame) throws AnalyticsRuntimeException {

		// Adding the required output columns to future frame.
		trendModelInputFrame = addRequiredOutputColumnsToFutureFrame(trendModelInputFrame);

		// Adding the required sell down indicator column to future frame.
		trendModelInputFrame = addSellDownIndToFutureFrame(trendModelInputFrame);

		// Adding the required new model year flag column to future frame.
		trendModelInputFrame = addNewModelYearFlagToFutureFrame(trendModelInputFrame);

		return trendModelInputFrame;
	}

	/**
	 * This method generates the created time stamp and add it to input frame.
	 * 
	 * @param inputFrame
	 * @param segmentVolume
	 * @return
	 */
	public List<String> generateCreatedTimestamp(DataFrame inputFrame) {
		// Collect data from the data frame.
		List<Row> rows = inputFrame.collectAsList();

		// Processed time stamp.
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				TrendingModelUtil.getValue("CREATE_TIMESTAMP_FORMAT"));
		String timeStamp = simpleDateFormat.format(new Date());
		logger.info("Processed timeStamp: " + timeStamp);

		// Adding time stamp details.
		List<String> timestampList = new ArrayList<>();
		for (int counter = 0; counter < rows.size(); counter++) {
			timestampList.add(timeStamp);
		}
		return timestampList;
	}

	/**
	 * This method generates the effective date value and add it in input frame.
	 * 
	 * @param inputFrame
	 * @param segmentVolume
	 * @return
	 */
	public List<String> generateEffectiveDate(DataFrame inputFrame) {
		// Collect data from the data frame.
		List<Row> rows = inputFrame.collectAsList();

		// Processed time stamp.
		SimpleDateFormat effectiveDateFormat = new SimpleDateFormat(
				TrendingModelUtil.getValue("EFFECTIVE_DATE_FORMAT"));
		String effectiveDate = effectiveDateFormat.format(new Date()).concat(
				TrendingModelUtil.getValue("EFFECTIVE_DATE_FORMAT_VALUE"));
		logger.info("Processed effectiveDate: " + effectiveDate);

		// Adding time stamp details.
		List<String> effectiveDateList = new ArrayList<>();
		for (int counter = 0; counter < rows.size(); counter++) {
			effectiveDateList.add(effectiveDate);
		}
		return effectiveDateList;
	}

	/**
	 * This methods reads the required columns data for the regression.
	 * 
	 * @param historyDataFrame
	 * @return
	 */
	public DataFrame readHistoryFrameData(DataFrame historyDataFrame) {
		return historyDataFrame
				.select(historyDataFrame.col(TrendingModelUtil
						.getValue("SERIES_ID")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("SERIES_NM")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("SEGMENT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("BUSINESS_MONTH")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("REGION_CODE")),
						historyDataFrame.col(TrendingModelUtil.getValue("MSRP")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("AVG_MSRP_CHG_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_AMT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_AMT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN_AMT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_PIN")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_PIN_AMT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("DEALER_GROSS_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("DEALER_GROSS_AMT")),
						historyDataFrame.col(TrendingModelUtil.getValue("CFTP")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("CFTP_PIN")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_RATIO")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("DEALER_MARGIN_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("DEALER_MARGIN")),
						historyDataFrame.col(TrendingModelUtil.getValue("DPH")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MAJOR_CHANGE")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MINOR_CHANGE")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("MONTHS_AFTER_LAUNCH")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR_SALES_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")),
						historyDataFrame.col(
								TrendingModelUtil
										.getValue("LATEST_MODEL_YEAR_OUTPUT"))
								.as(TrendingModelUtil
										.getValue("LATEST_MODEL_YEAR")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME")), historyDataFrame
								.col(TrendingModelUtil.getValue("SEG_SHARE")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("SEGMENT_NON_FLEET_PCT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_ANNUAL")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("TMS_INCTV_BUDGET_AMT")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("CHANGE_CATEGORY")), historyDataFrame
								.col(TrendingModelUtil
										.getValue("LAUNCH_MONTH_IND")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("GAS_PRICE_IDX")), historyDataFrame
								.col(TrendingModelUtil
										.getValue("SELL_DOWN_IND")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("POSITIVE_SPECIAL_EVENT_IND")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("NEGATIVE_SPECIAL_EVENT_IND")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("THRUST_EVENT_IND")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("CROSS_IMPACT_INDEX")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("FLEET_VOLUME_TARGET")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("SALES_TARGET")), historyDataFrame
								.col(TrendingModelUtil
										.getValue("MSRP_ELASTICITY")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("INCTV_ELASTICITY")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("FLEET_PCT")), historyDataFrame
								.col(TrendingModelUtil.getValue("INCTV_RATIO")));
	}

	/**
	 * This method reads the required columns data for the forecasting.
	 * 
	 * @param futureDataFrame
	 * @return
	 */
	public DataFrame readFutureFrameData(DataFrame futureDataFrame) {
		return futureDataFrame
				.select(futureDataFrame.col(TrendingModelUtil
						.getValue("SERIES_ID")), futureDataFrame
						.col(TrendingModelUtil.getValue("SERIES_NM")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("SEGMENT")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("REGION_CODE")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("BUSINESS_MONTH")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("TMS_INCTV_BUDGET_AMT")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("CHANGE_CATEGORY")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("LAUNCH_MONTH_IND")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("GAS_PRICE_IDX")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("SEGMENT_VOLUME_ANNUAL")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("POSITIVE_SPECIAL_EVENT_IND")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("NEGATIVE_SPECIAL_EVENT_IND")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("THRUST_EVENT_IND")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("CROSS_IMPACT_INDEX")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("FLEET_VOLUME_TARGET")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("SALES_TARGET")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("DEALER_MARGIN_PCT")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("LATEST_MODEL_YEAR_OUTPUT")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_RATIO")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("YEARS_SINCE_MAJOR_CHANGE")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MINOR_CHANGE")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("MONTHS_AFTER_LAUNCH")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("SELL_DOWN_IND")), futureDataFrame
								.col(TrendingModelUtil
										.getValue("MSRP_ELASTICITY")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("INCTV_ELASTICITY")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("ENTERPRISE_CM_PER_UNIT_AMT")),
						futureDataFrame.col(TrendingModelUtil
								.getValue("FLEET_PCT")), futureDataFrame
								.col(TrendingModelUtil.getValue("INCTV_RATIO")));
	}

	/**
	 * This method returns the current date format like yyyyMM.
	 * 
	 * @return
	 */
	public String generateCurrentDate(DataFrame historyFrame) {
		String businessMonth = "";
		int msrpamount = 0;
		historyFrame = historyFrame.filter("msrp_amt > " + msrpamount);
		historyFrame.show();
		DataFrame businessMonthFrame = historyFrame
				.select(max("business_month"));
		if (businessMonthFrame.count() > 0) {
			Row row = businessMonthFrame.first();
			businessMonth = row.isNullAt(0) ? "" : row.getString(0);
		}
		logger.info("MSRP BusinessMonth: " + businessMonth);
		return businessMonth;
	}

}
