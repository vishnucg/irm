/**
 * 
 */
package com.toyota.analytics.trend.postprocessing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.collection.JavaConversions;

import com.toyota.analytics.common.util.TrendingModelUtil;
import com.toyota.analytics.trend.datapreparation.TrendDataPreparation;

/**
 * This class contains the methods which can contains the methods will create
 * the output tables and will put model forecast data into output table. This
 * class creates the output table and put the history data and future data into
 * output table.
 * 
 * @author Naresh
 *
 */
public class TrendModelPostProcessor {

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(TrendModelPostProcessor.class);

	/**
	 * This method get the hive context, creates the trend model output table.
	 * This collects the required columns data from future frame and history
	 * frame and will be saved into output table. Here, Once all trend models
	 * process is completed, finally we will add created_ts and effective_date
	 * to both frames and will append data to output table.
	 * 
	 * @param futureFrame
	 * @param forecastMap
	 * @param historyOutputFrame
	 */
	public void saveForecastData(DataFrame futureFrame,
			Map<String, List<Double>> forecastMap, DataFrame historyOutputFrame) {

		// Get hive context and connect to hive tables.
		HiveContext hiveContext = TrendingModelUtil.getHivecontext();
		TrendDataPreparation dataPreparation = new TrendDataPreparation();

		// Creating the output table to put history and future data.
		hiveContext.sql(TrendingModelUtil.getValue("TREND_OUTPUT_TABLE_QUERY"));

		// Taking the all forecast values and create the columns and saving into
		// data base.
		futureFrame = addOutputColumnsToDataFrame(futureFrame.collectAsList(),
				futureFrame, forecastMap.entrySet());

		// Adding time stamp values to frame.
		futureFrame = addTimestampToDataFrame(futureFrame.collectAsList(),
				futureFrame,
				dataPreparation.generateCreatedTimestamp(futureFrame),
				TrendingModelUtil.getValue("RUN_TIMESTAMP"));

		// Adding effective date to frame.
		futureFrame = addTimestampToDataFrame(futureFrame.collectAsList(),
				futureFrame,
				dataPreparation.generateEffectiveDate(futureFrame),
				TrendingModelUtil.getValue("EFFECTIVE_DATE"));

		// Save future frame data to output table.
		futureFrame.show();
		DataFrame outputFrame = saveFutureData(futureFrame);
		outputFrame.saveAsTable(
				TrendingModelUtil.getValue("TREND_OUTPUT_TABLE_NAME"),
				SaveMode.Append);
	}

	/**
	 * This method adds the segment_seasonal factors, fleet_pct, create_ts and
	 * effective_date.
	 * 
	 * @param historyDataFrame
	 * @param forecastMap
	 * @return
	 */
	public DataFrame addSesonalFactorsFleetPctToHistoryFrame(
			DataFrame historyDataFrame, Map<String, List<Double>> forecastMap) {
		TrendDataPreparation dataPreparation = new TrendDataPreparation();
		// Taking the all forecast values and create the columns and saving into
		// data base.
		historyDataFrame = addOutputColumnsToDataFrame(
				historyDataFrame.collectAsList(), historyDataFrame,
				forecastMap.entrySet());

		// Adding time stamp values to frame.
		historyDataFrame = addTimestampToDataFrame(
				historyDataFrame.collectAsList(), historyDataFrame,
				dataPreparation.generateCreatedTimestamp(historyDataFrame),
				TrendingModelUtil.getValue("RUN_TIMESTAMP"));

		// Adding effective date to frame.
		historyDataFrame = addTimestampToDataFrame(
				historyDataFrame.collectAsList(), historyDataFrame,
				dataPreparation.generateEffectiveDate(historyDataFrame),
				TrendingModelUtil.getValue("EFFECTIVE_DATE"));
		return historyDataFrame;
	}

	/**
	 * This method adds the calculated output columns to data frame.
	 * 
	 * @param rows
	 * @param trendInputFrame
	 * @param values
	 * @return
	 */
	public DataFrame addOutputColumnsToDataFrame(List<Row> rows,
			DataFrame trendInputFrame,
			Set<Map.Entry<String, List<Double>>> values) {
		logger.info("addColumnToDataFrame start");
		if (null != rows && !rows.isEmpty()) {
			List<Row> rowsWithForecastValues = new ArrayList<>(rows.size());

			// Reading the forecast values.
			logger.info("Number of rows in frame: " + rows.size());
			for (int j = 0; j < rows.size(); j++) {
				List<Object> newRow = new ArrayList<>(
						JavaConversions.asJavaList(rows.get(j).toSeq()));
				for (Map.Entry<String, List<Double>> entry : values) {
					List<Double> listValues = entry.getValue();
					newRow.add(listValues.get(j));
				}
				rowsWithForecastValues.add(RowFactory.create(newRow
						.toArray(new Object[newRow.size()])));
			}

			// Building the RDD based on the values.
			JavaRDD<Row> rowJavaRDD = TrendingModelUtil.getJavaSparkContext()
					.parallelize(rowsWithForecastValues);

			// Creating the fields as per the columns.
			List<StructField> structFields = new ArrayList<>(
					JavaConversions.asJavaList(trendInputFrame.schema()));
			for (Map.Entry<String, List<Double>> entry : values) {
				String columnName = entry.getKey();
				structFields.add(new StructField(columnName,
						DataTypes.DoubleType, true, Metadata.empty()));
			}

			// Creating the data frame based on the RDD and struct type.
			StructType structType = new StructType(
					structFields.toArray(new StructField[structFields.size()]));
			trendInputFrame = TrendingModelUtil.getHivecontext()
					.createDataFrame(rowJavaRDD, structType);
		}
		return trendInputFrame;
	}

	/**
	 * This method adds the time stamp value to frame.
	 * 
	 * @param rows
	 * @param trendInputFrame
	 * @param listValues
	 * @return
	 */
	private DataFrame addTimestampToDataFrame(List<Row> rows,
			DataFrame trendInputFrame, List<String> listValues,
			String columnName) {
		logger.info("addTimestampToDataFrame start");
		if (null != rows && !rows.isEmpty()) {
			List<Row> rowsWithTimestampValues = new ArrayList<>(rows.size());

			// Reading the time stamp values.
			for (int j = 0; j < rows.size(); j++) {
				List<Object> newRow = new ArrayList<>(
						JavaConversions.asJavaList(rows.get(j).toSeq()));
				newRow.add(listValues.get(j));
				rowsWithTimestampValues.add(RowFactory.create(newRow
						.toArray(new Object[newRow.size()])));
			}
			// Adding the time stamp values to frame.
			JavaRDD<Row> rowJavaRDD = TrendingModelUtil.getJavaSparkContext()
					.parallelize(rowsWithTimestampValues);
			List<StructField> structFields = new ArrayList<>(
					JavaConversions.asJavaList(trendInputFrame.schema()));
			structFields.add(new StructField(columnName, DataTypes.StringType,
					true, Metadata.empty()));
			StructType structType = new StructType(
					structFields.toArray(new StructField[structFields.size()]));
			trendInputFrame = TrendingModelUtil.getHivecontext()
					.createDataFrame(rowJavaRDD, structType);
		}
		return trendInputFrame;
	}

	/**
	 * This method saves the history frame data to output table. Here, few of
	 * the columns calculated other columns taken from direct input frame. These
	 * are the columns calculated based on the history frame input data.
	 * created_ts, effective_date, years_since_major_change,
	 * years_since_minor_change, sell_down_ind, segment_seasonal_factor_month_1,
	 * segment_seasonal_factor_month_2, segment_seasonal_factor_month_3,
	 * segment_seasonal_factor_month_4, segment_seasonal_factor_month_5,
	 * segment_seasonal_factor_month_6, segment_seasonal_factor_month_7,
	 * segment_seasonal_factor_month_8, segment_seasonal_factor_month_9,
	 * segment_seasonal_factor_month_10, segment_seasonal_factor_month_11,
	 * segment_seasonal_factor_month_12, fleet_pct
	 * 
	 * Other columns collected from the history data frame.
	 * 
	 * 
	 * @param finalOutputFrame
	 * @return
	 */
	public DataFrame saveHistoryData(DataFrame finalOutputFrame) {
		// Selecting the required data from the future forecast frame and save
		// into output table.
		return finalOutputFrame
				.select(finalOutputFrame.col(TrendingModelUtil
						.getValue("RUN_TIMESTAMP")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SERIES_NM")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("REGION_CODE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("EFFECTIVE_DATE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("BUSINESS_MONTH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MSRP_ELASTICITY")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCTV_ELASTICITY")),
						finalOutputFrame.col(TrendingModelUtil.getValue("MSRP")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("AVG_MSRP_CHG_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("TMS_INCTV_BUDGET_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_PIN")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_PIN_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DEALER_GROSS_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DEALER_GROSS_AMT")),
						finalOutputFrame.col(TrendingModelUtil.getValue("CFTP")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("CFTP_PIN")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("INCENTIVE_RATIO")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCTV_RATIO")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("DEALER_MARGIN_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DEALER_MARGIN")), finalOutputFrame
								.col(TrendingModelUtil.getValue("DPH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("CHANGE_CATEGORY")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("LAUNCH_MONTH_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MAJOR_CHANGE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MINOR_CHANGE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MONTHS_AFTER_LAUNCH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR_SALES_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("LATEST_MODEL_YEAR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("GAS_PRICE_IDX")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("SELL_DOWN_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_JAN_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_FEB_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_MAR_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_APR_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_MAY_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_JUNE_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_JULY_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_AUG_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_SEP_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_OCT_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_NOV_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_DEC_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("SEGMENT_VOLUME_ANNUAL")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEG_SHARE")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("SEGMENT_NON_FLEET_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("POSITIVE_SPECIAL_EVENT_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("NEGATIVE_SPECIAL_EVENT_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("THRUST_EVENT_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("CROSS_IMPACT_INDEX")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("FLEET_VOLUME_TARGET")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("FLEET_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SALES_TARGET")));

	}

	/**
	 * This method saves the future frame data to output table. Here, these are
	 * the columns we calculated and rest of the columns taken from the future
	 * frame. We calculated all the fields and added to the future frame.
	 * 
	 * created_ts, effective_date, msrp_amt, avg_msrp_amt_chg_pct,
	 * tms_inctv_pct, tms_inctv_amt, increasing_tms_inctv_pct,
	 * increasing_tms_inctv_amt, pin_inctv_pct, pin_inctv_amt,
	 * increasing_pin_inctv_pct, increasing_pin_inctv_amt, dealer_gross_pct,
	 * dealer_gross_amt, cftp_amt, cftp_pin_amt, incentive_ratio,
	 * dealer_margin_pct, dealer_margin_amt, dph_amt, years_since_major_change,
	 * years_since_minor_change, months_after_launch_nbr, months_b4_next_launch,
	 * model_year_sales_pct, new_model_year_flag, latest_model_year,
	 * sell_down_ind, segment_seasonal_factor_month_1,
	 * segment_seasonal_factor_month_2, segment_seasonal_factor_month_3,
	 * segment_seasonal_factor_month_4, segment_seasonal_factor_month_5,
	 * segment_seasonal_factor_month_6, segment_seasonal_factor_month_7,
	 * segment_seasonal_factor_month_8, segment_seasonal_factor_month_9,
	 * segment_seasonal_factor_month_10, segment_seasonal_factor_month_11,
	 * segment_seasonal_factor_month_12, segment_volume, pin_seg_share,
	 * retail_seg_share, segment_non_fleet_pct, fleet_pct
	 * 
	 * @param finalOutputFrame
	 * @return
	 */
	private DataFrame saveFutureData(DataFrame finalOutputFrame) {
		// Selecting the required data from the future forecast frame and save
		// into output table.
		return finalOutputFrame
				.select(finalOutputFrame.col(TrendingModelUtil
						.getValue("RUN_TIMESTAMP")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SERIES_NM")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("REGION_CODE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("EFFECTIVE_DATE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("BUSINESS_MONTH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MSRP_ELASTICITY")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCTV_ELASTICITY")),
						finalOutputFrame.col(TrendingModelUtil.getValue("MSRP")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("AVG_MSRP_CHG_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("TMS_INCTV_BUDGET_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_OUTPUT_DOLLERS")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_OUTPUT_DOLLERS")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_PIN_OUTPUT_DOLLERS")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_PIN_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCENTIVE_PCT_NONDESC_PIN_OUTPUT_DOLLERS")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DEALER_GROSS_PCT_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DEALER_GROSS_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil.getValue("CFTP")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("CFTP_PIN_OUTPUT")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("INCENTIVE_RATIO")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("INCTV_RATIO")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("DEALER_MARGIN_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DEALER_MARGIN_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("DPH_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("ENTERPRISE_CM_PER_UNIT_AMT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("CHANGE_CATEGORY")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("LAUNCH_MONTH_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MAJOR_CHANGE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MINOR_CHANGE")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MONTHS_AFTER_LAUNCH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR_SALES_PCT_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("LATEST_MODEL_YEAR_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("GAS_PRICE_IDX")), finalOutputFrame
								.col(TrendingModelUtil
										.getValue("SELL_DOWN_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_JAN_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_FEB_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_MAR_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_APR_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_MAY_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_JUNE_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_JULY_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_AUG_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_SEP_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_OCT_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_NOV_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_DEC_FACTOR")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_ANNUAL")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEG_SHARE_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SEGMENT_NON_FLEET_PCT_OUTPUT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("POSITIVE_SPECIAL_EVENT_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("NEGATIVE_SPECIAL_EVENT_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("THRUST_EVENT_IND")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("CROSS_IMPACT_INDEX")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("FLEET_VOLUME_TARGET")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("FLEET_PCT")),
						finalOutputFrame.col(TrendingModelUtil
								.getValue("SALES_TARGET")));
	}
}
