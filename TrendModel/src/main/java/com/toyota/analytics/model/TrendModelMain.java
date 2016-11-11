/**
 * 
 */
package com.toyota.analytics.model;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;
import com.toyota.analytics.common.util.TrendingModelUtil;
import com.toyota.analytics.trend.datapreparation.TrendDataPreparation;
import com.toyota.analytics.trend.trendcalculations.TrendCalculationsDTO;
import com.toyota.analytics.trend.trendcalculations.TrendModelInputDetails;
import com.toyota.analytics.trend.trendcalculations.TrendModelManager;

/**
 * This class is the entry point for the TrendModels calculations. This connects
 * to hive, reads the trend model input data and divided as history data and
 * future data based on the current date for doing forecasting. All these frames
 * are ordered by series_id and business_month. We need to calculate the
 * years_since_major_change, years_since_minor_change, months_after_launch_nbr,
 * months_b4_next_launch, latest_model_year, sell_down_ind, new_model_year_flag
 * and add to trend model input frame before divide into history and future
 * frame.
 * 
 * We loop through the each series and taking the max_model_year, max
 * business_month, last row DPH and previous MSRP Value for later MSRP and DPH
 * calculations. Then, filters the future frame also with the same series and
 * pass it to trend calculations. Before pass it to trend calculations will
 * perform minimum data validations on the history frame.
 * 
 * @author Naresh
 *
 */
public class TrendModelMain {

	// initialize the logger.
	private static final Logger logger = Logger.getLogger(TrendModelMain.class);

	/**
	 * This method is the entry point for the TrendModels calculations. This
	 * connects to hive, reads the trend model input data and divided as history
	 * data and future data based on the current date for doing forecasting. All
	 * these frames are ordered by series_id and business_month. We need to
	 * calculate the years_since_major_change, years_since_minor_change,
	 * months_after_launch_nbr, months_b4_next_launch, latest_model_year,
	 * sell_down_ind, new_model_year_flag and add to trend model input frame
	 * before divide into history and future frame.
	 * 
	 * We loop through the each series and taking the max_model_year, max
	 * business_month, last row DPH and previous MSRP Value for later MSRP and
	 * DPH calculations. Then, filters the future frame also with the same
	 * series and pass it to trend calculations. Before pass it to trend
	 * calculations will perform minimum data validations on the history frame.
	 * 
	 * 
	 * @param args
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public static void main(String[] args) throws IOException,
			AnalyticsRuntimeException {
		logger.info("Trend Models started.");

		// Initializing the model manager.
		TrendModelManager trendModelManager = new TrendModelManager();
		TrendDataPreparation dataPreparation = new TrendDataPreparation();
		TrendCalculationsDTO trendsInputInformation = new TrendCalculationsDTO();
		trendsInputInformation.setHiveContext(TrendingModelUtil
				.getHivecontext());
		trendsInputInformation.getHiveContext().sql("use " + args[0]);

		// Reading the the trend model input. Here, will get all series data.
		logger.info("Reading the trend model input started.");
		DataFrame trendModelInputFrame = dataPreparation
				.getTrendModelsInput(trendsInputInformation);

		// Ordering the trend model input data by series_id and business_month.
		DataFrame orderedTrendInput = trendModelInputFrame.sort(
				trendModelInputFrame.col(TrendingModelUtil
						.getValue("SERIES_ID")), trendModelInputFrame
						.col(TrendingModelUtil.getValue("BUSINESS_MONTH")));

		// Taking the all the distinct series from the history frame.
		Row[] values = orderedTrendInput
				.select(orderedTrendInput.col(TrendingModelUtil
						.getValue("SERIES"))).distinct().collect();

		// Processing each series history and future data.
		for (Row row : values) {
			String series = row.getString(0);
			logger.info("series: " + series);

			// This filters the future data by using the series id.
			DataFrame seriesData = orderedTrendInput.filter(orderedTrendInput
					.col(TrendingModelUtil.getValue("SERIES")).equalTo(series));
			logger.info("Before seriesData: " + seriesData.count());

			if (seriesData.count() > 0) {

				// Removing the unnecessary rows from the frame.
				seriesData = trendModelManager.removeUncessaryRows(seriesData);
				logger.info("After seriesData: " + seriesData.count());

				// Calculates and adding the years_since_major_change,
				// years_since_minor_change,months_after_launch_nbr,months_b4_next_launch,
				// latest_model_year, new_model_year_flag and sell_down_ind to
				// future
				// frame.
				DataFrame inputSeriesData = dataPreparation
						.addOutputColumnsToFutureFrame(seriesData);

				// Dividing the Trend model input frame into history and future
				// frame.
				// reads history data before current date.
				DataFrame historyDataFrame = inputSeriesData
						.filter(inputSeriesData
								.col(TrendingModelUtil
										.getValue("BUSINESS_MONTH"))
								.$less$eq(
										dataPreparation
												.generateCurrentDate(inputSeriesData)));
				historyDataFrame = dataPreparation
						.readHistoryFrameData(historyDataFrame);

				// This method reads the each series data from history and take
				// the
				// required information like max business month, max model year,
				// previous MSRP value and DPH Value.
				TrendModelInputDetails inputDetails = dataPreparation
						.prepareProcessRequiredSeriesData(historyDataFrame);

				// Logging input values.
				logger.info("Previous MSRP Value: "
						+ inputDetails.getPreviousMsrpValue()
						+ " : Max Model Year: "
						+ inputDetails.getMaxModelYear() + " : DPH Value: "
						+ inputDetails.getDphValue());

				// Running minimum data validation rules on history data
				// frame
				// for each series.
				// Reads the future data greater than equal to current date.
				DataFrame futureDataFrame = inputSeriesData
						.filter(inputSeriesData
								.col(TrendingModelUtil
										.getValue("BUSINESS_MONTH"))
								.$greater(
										dataPreparation
												.generateCurrentDate(inputSeriesData)));
				futureDataFrame = dataPreparation
						.readFutureFrameData(futureDataFrame);
				logger.info("Reading the trend model input ended.");

				if (futureDataFrame.count() > 0) {
					try {
						// Running the trend models based on the history
						// data
						// frame
						// and
						// forecast the data based on the future data frame.
						trendModelManager.runTrendModels(historyDataFrame,
								futureDataFrame,
								inputDetails.getPreviousMsrpValue(),
								inputDetails.getDphValue(),
								inputDetails.getMaxModelYear());
					} catch (AnalyticsRuntimeException e) {
						logger.error("Series Name: " + series + " Problem: "
								+ e.getMessage());
					}
				} else {
					logger.warn("There is no future data is available for this series: "
							+ series);
				}
			} else {
				logger.warn("There is no history data is available for this series: "
						+ series);
			}
		}

		// Finally closing the session context.
		TrendingModelUtil.closeJavaSparkContext();
		logger.info("Trend Models ended.");
	}
}
