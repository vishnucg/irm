/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import com.toyota.analytics.common.regression.LinearModel;
import com.toyota.analytics.common.regression.LinearModelDTO;
import com.toyota.analytics.common.util.TrendingModelUtil;
import com.toyota.analytics.trend.datapreparation.TrendDataPreparation;
import com.toyota.analytics.trend.postprocessing.TrendModelPostProcessor;

/**
 * 
 * This class contains the methods and runs all the trend models and provides
 * the forecast values. It collects all the models dependent and independent
 * variables and starts the process. You can see all the variables list in
 * messages.properties file.
 * 
 * First, We run the MSRPModel, in this first based on the dependent and
 * independent variables and previous MSRP Value we will start the calculation.
 * We calculate the average MSRP from the history frame and then calculates the
 * average MSRP percent over the previous year. Once we got the MSRP Percent
 * change over previous year, will do regression on the avg_msrp_amt_chg_pct as
 * dependent and major_change,minor_change,new_model_year_flag as independent
 * list. We will calculates the intercept and coefficients for each column back.
 * By using the intercept and coefficients we will forecast the data using
 * future frame.
 * 
 * Second, We run the Incentive Model,in this first will do regression on the
 * history input by using these variables
 * tms_inctv_pct,increasing_tms_inctv_pct,pin_inctv_pct,
 * increasing_pin_inctv_pct as dependent variables and
 * years_since_major_change,months_after_launch_nbr,months_b4_next_launch as
 * independent list. We will measure the intercept and coefficients for each
 * column. By using the intercept and coefficients we will forecast the data
 * using future frame and will add calculated values to future frame.
 * 
 * Third, We run the dealer gross Model, in this first will do regression on the
 * history input by using these variables dealer_gross_pct as dependent variable
 * and years_since_major_change,months_after_launch_nbr, months_b4_next_launch
 * as independent list. We will measure the intercept and coefficients for each
 * column. By using the intercept and coefficients we will forecast the data
 * using future frame and will add calculated dealer_gross_pct and
 * dealer_gross_amt values to future frame.
 * 
 * Fourth, We will calculate the DPH Value based on the previous row of each
 * series and also calculates the CFTP ,CFTP Pin based on the formula and using
 * regression. We calculated CFTP like this (MSRP-increasing_tms_inctv_amt+DPH).
 * For CFTP Pin, will do regression by using the history frame, cftp_pin_amt as
 * dependent variable and
 * years_since_major_change,months_after_launch_nbr,new_model_year_flag as
 * independent list and calculates the CFTP Pin forecast values and finally
 * calculated DPH, CFTP and CFTP Pin values add to future frame.
 * 
 * Fifth, We will calculates the seasonal factors, segment volume, pin_seg_share
 * and retail_seg_share and added to future frame.
 * 
 * @author Naresh
 *
 */
public class TrendModelManager implements Serializable {

	private static final long serialVersionUID = 459156777826591573L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(TrendModelManager.class);

	/**
	 * This method runs all the trend models and provides the forecast values.
	 * It collects all the models dependent and independent variables and starts
	 * the process. You can see all the variables list in messages.properties
	 * file.
	 * 
	 * First, We run the MSRPModel, in this first based on the dependent and
	 * independent variables and previous MSRP Value we will start the
	 * calculation. We calculate the average MSRP from the history frame and
	 * then calculates the average MSRP percent over the previous year. Once we
	 * got the MSRP Percent change over previous year, will do regression on the
	 * avg_msrp_amt_chg_pct as dependent and
	 * major_change,minor_change,new_model_year_flag as independent list. We
	 * will calculates the intercept and coefficients for each column back. By
	 * using the intercept and coefficients we will forecast the data using
	 * future frame.
	 * 
	 * Second, We run the Incentive Model,in this first will do regression on
	 * the history input by using these variables
	 * tms_inctv_pct,increasing_tms_inctv_pct,pin_inctv_pct,
	 * increasing_pin_inctv_pct as dependent variables and
	 * years_since_major_change,months_after_launch_nbr,months_b4_next_launch as
	 * independent list. We will measure the intercept and coefficients for each
	 * column. By using the intercept and coefficients we will forecast the data
	 * using future frame and will add calculated values to future frame.
	 * 
	 * Third, We run the dealer gross Model, in this first will do regression on
	 * the history input by using these variables dealer_gross_pct as dependent
	 * variable and years_since_major_change,months_after_launch_nbr,
	 * months_b4_next_launch as independent list. We will measure the intercept
	 * and coefficients for each column. By using the intercept and coefficients
	 * we will forecast the data using future frame and will add calculated
	 * dealer_gross_pct and dealer_gross_amt values to future frame.
	 * 
	 * Fourth, We will calculate the DPH Value based on the previous row of each
	 * series and also calculates the CFTP ,CFTP Pin based on the formula and
	 * using regression. We calculated CFTP like this
	 * (MSRP-increasing_tms_inctv_amt+DPH). For CFTP Pin, will do regression by
	 * using the history frame, cftp_pin_amt as dependent variable and
	 * years_since_major_change,months_after_launch_nbr,new_model_year_flag as
	 * independent list and calculates the CFTP Pin forecast values and finally
	 * calculated DPH, CFTP and CFTP Pin values add to future frame.
	 * 
	 * Fifth, We will calculates the seasonal factors, segment volume,
	 * pin_seg_share and retail_seg_share and added to future frame.
	 * 
	 * @param historyDataFrame
	 * @param futureDataFrame
	 * @param previousMsrpValue
	 * @param maxModelYearForPreviousMonth
	 * @param previousDphValue
	 * @param maxModelYear
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public void runTrendModels(DataFrame historyDataFrame,
			DataFrame futureDataFrame, double previousMsrpValue,
			double previousDphValue, int maxModelYear) throws IOException,
			AnalyticsRuntimeException {

		// Reading the dependent and independent variables list for regression.
		TrendCalculationsDTO trendModelsDTO = TrendingModelUtil
				.readProperties();
		Map<String, List<Double>> forecastMap = new LinkedHashMap<>();

		// Calling the MSRP Model.
		try {
			logger.info("MSRP Forecast Model is started.");
			DataFrame forecastOutputFrame = runMsrpTrend(historyDataFrame,
					futureDataFrame, previousMsrpValue, trendModelsDTO,
					maxModelYear);
			logger.info("MSRP Forecast Model is ended.");

			// Calling the Incentive Model.
			logger.info("Incentive Forecast Model is started.");
			forecastOutputFrame = runIncentiveTrend(forecastOutputFrame,
					trendModelsDTO, historyDataFrame);
			logger.info("Incentive Forecast Model is ended.");

			// Calculating the model year sales percentage.
			logger.info("modelYearSalesPercent is started.");
			Map<Integer, Double> averageValues = calculateModelYearSalesPct(historyDataFrame);
			List<Double> modelYearSalesPercent = forecastModelYearSalesPct(
					futureDataFrame, averageValues);
			forecastOutputFrame = addColumnToForecastFrame(forecastOutputFrame,
					modelYearSalesPercent,
					TrendingModelUtil.getValue("MODEL_YEAR_SALES_SPLIT")
							+ TrendingModelUtil.getValue("OUTPUT"));
			logger.info("modelYearSalesPercent: " + modelYearSalesPercent);
			logger.info("modelYearSalesPercent is ended.");

			// Calling the Dealer Gross Model.
			forecastOutputFrame = runDealerGrossTrend(forecastOutputFrame,
					historyDataFrame, trendModelsDTO);

			// Calling the DPH Model and CFTP ,CFTP Pin calculations.
			forecastOutputFrame = runDPHCFTPTrend(forecastOutputFrame,
					previousDphValue, trendModelsDTO, historyDataFrame);

			// Adding segment volume details.
			logger.info("Segment Volume Calculation started.");
			SegmentCalculations segmentCalculations = new SegmentCalculations();
			forecastMap = segmentCalculations.addColumnValues(
					forecastOutputFrame, forecastMap, historyDataFrame);
			logger.info("Segment Volume Calculation ended.");

			// Saving the forecast to output table.
			saveForecastOutputData(forecastOutputFrame, forecastMap,
					historyDataFrame);
		} catch (AnalyticsRuntimeException exception) {
			throw new AnalyticsRuntimeException(exception.getMessage());
		}
	}

	/**
	 * We run the MSRPModel, in this first based on the dependent and
	 * independent variables and previous MSRP Value we will start the
	 * calculation. We calculate the average MSRP from the history frame and
	 * then calculates the average MSRP percent over the previous year. Once we
	 * got the MSRP Percent change over previous year, will do regression on the
	 * avg_msrp_amt_chg_pct as dependent and major_change,minor_change,
	 * new_model_year_flag as independent list. We will calculates the intercept
	 * and coefficients for each column back. By using the intercept and
	 * coefficients we will forecast the data using future frame.
	 * 
	 * 
	 * @param historyDataFrame
	 * @param futureDataFrame
	 * @param previousMsrpValue
	 * @param trendModelsDTO
	 * @param maxModelYear
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private DataFrame runMsrpTrend(DataFrame historyDataFrame,
			DataFrame futureDataFrame, double previousMsrpValue,
			TrendCalculationsDTO trendModelsDTO, int maxModelYear)
			throws IOException, AnalyticsRuntimeException {
		// Calling the MSRP Model.
		logger.info("MSRP Forecast Model is started.");
		DataFrame forecastOutputFrame = null;
		try {
			Map<String, List<Double>> msrpForecastValues = runMSRPModel(
					historyDataFrame, futureDataFrame, previousMsrpValue,
					trendModelsDTO.getMsrpDependantVar(),
					trendModelsDTO.getMsrpIndependentColumnList(), maxModelYear);

			// Adding columns to forecast output frame for each model.
			logger.info("Adding MSRP columns to output frame is started.");
			forecastOutputFrame = addColumnToForecastFrame(futureDataFrame,
					msrpForecastValues.get(TrendingModelUtil
							.getValue("AVG_MSRP_CHG_PCT")),
					TrendingModelUtil.getValue("AVG_MSRP_CHG_PCT"));
			forecastOutputFrame = addColumnToForecastFrame(forecastOutputFrame,
					msrpForecastValues.get(TrendingModelUtil.getValue("MSRP")),
					TrendingModelUtil.getValue("MSRP"));
		} catch (AnalyticsRuntimeException analyticsRuntimeException) {
			throw new AnalyticsRuntimeException(
					analyticsRuntimeException.getMessage());
		}
		logger.info("Adding MSRP columns to output frame is completed.");
		logger.info("MSRP Forecast Model is completed.");
		return forecastOutputFrame;
	}

	/**
	 * We run the Incentive Model,in this first will do regression on the
	 * history input by using these variables
	 * tms_inctv_pct,increasing_tms_inctv_pct,pin_inctv_pct,
	 * increasing_pin_inctv_pct as dependent variables and
	 * years_since_major_change,months_after_launch_nbr,months_b4_next_launch as
	 * independent list. We will measure the intercept and coefficients for each
	 * column. By using the intercept and coefficients we will forecast the data
	 * using future frame and will add calculated values to future frame.
	 * 
	 * @param forecastOutputFrame
	 * @param trendModelsDTO
	 * @param historyDataFrame
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private DataFrame runIncentiveTrend(DataFrame forecastOutputFrame,
			TrendCalculationsDTO trendModelsDTO, DataFrame historyDataFrame)
			throws IOException, AnalyticsRuntimeException {
		logger.info("Incentive Forecast Model is started.");
		try {
			Map<String, List<Double>> incentiveOutputColumns = new LinkedHashMap<>();
			logger.info("runIncentiveTrend Befoe check the data in frame: "
					+ trendModelsDTO.getIncentiveIndependentColumnList());
			List<String> indepList = TrendingModelUtil.checkColumnsData(
					historyDataFrame,
					trendModelsDTO.getIncentiveIndependentColumnList());
			trendModelsDTO.setIncentiveIndependentColumnList(indepList);
			logger.info("runIncentiveTrend After Befoe check the data in frame: "
					+ indepList);
			IncentiveTrendModelManager incentiveTrendModelManager = new IncentiveTrendModelManager();
			for (String dependentVariable : trendModelsDTO
					.getDependentVarsList()) {
				double previousInctiveValue = incentiveTrendModelManager
						.collectPreviousInctvPct(historyDataFrame,
								dependentVariable);
				logger.info("previousInctiveValue: " + previousInctiveValue);
				incentiveOutputColumns.put(
						dependentVariable
								+ TrendingModelUtil.getValue("OUTPUT"),
						runIncentiveModel(historyDataFrame,
								forecastOutputFrame, trendModelsDTO,
								dependentVariable, previousInctiveValue));
			}
			logger.info("IncentiveOutputColumns Map:." + incentiveOutputColumns);

			// Add incentive output columns to Data frame.
			TrendModelPostProcessor trendModelPostProcessor = new TrendModelPostProcessor();
			forecastOutputFrame = trendModelPostProcessor
					.addOutputColumnsToDataFrame(
							forecastOutputFrame.collectAsList(),
							forecastOutputFrame,
							incentiveOutputColumns.entrySet());

			// Adding the increasing incentive percent values.
			Map<String, List<Double>> increasingIncentivies = new LinkedHashMap<>();
			increasingIncentivies.put(
					TrendingModelUtil.getValue("INCENTIVE_PCT_NONDESC")
							+ TrendingModelUtil.getValue("OUTPUT"),
					incentiveTrendModelManager.calculateIncreasingInctvValues(
							forecastOutputFrame,
							TrendingModelUtil.getValue("INCENTIVE_PCT")
									+ TrendingModelUtil.getValue("OUTPUT")));
			increasingIncentivies.put(
					TrendingModelUtil.getValue("INCENTIVE_PCT_NONDESC_PIN")
							+ TrendingModelUtil.getValue("OUTPUT"),
					incentiveTrendModelManager.calculateIncreasingInctvValues(
							forecastOutputFrame,
							TrendingModelUtil.getValue("INCENTIVE_PCT_PIN")
									+ TrendingModelUtil.getValue("OUTPUT")));
			forecastOutputFrame = trendModelPostProcessor
					.addOutputColumnsToDataFrame(
							forecastOutputFrame.collectAsList(),
							forecastOutputFrame,
							increasingIncentivies.entrySet());

			Map<String, List<Double>> incentiveOutputDollarColumns = new LinkedHashMap<>();
			for (String dependentVariable : trendModelsDTO
					.getDependentVarsList()) {
				// Predict the output values in dollars.
				incentiveOutputDollarColumns
						.put(dependentVariable
								+ TrendingModelUtil.getValue("OUTPUT_DOLLARS"),
								incentiveTrendModelManager
										.predictIncentiveOutput(
												forecastOutputFrame,
												dependentVariable
														+ TrendingModelUtil
																.getValue("OUTPUT")));
			}
			incentiveOutputDollarColumns.put(
					TrendingModelUtil.getValue("INCENTIVE_PCT_NONDESC")
							+ TrendingModelUtil.getValue("OUTPUT_DOLLARS"),
					incentiveTrendModelManager.predictIncentiveOutput(
							forecastOutputFrame,
							TrendingModelUtil.getValue("INCENTIVE_PCT_NONDESC")
									+ TrendingModelUtil.getValue("OUTPUT")));
			incentiveOutputDollarColumns.put(
					TrendingModelUtil.getValue("INCENTIVE_PCT_NONDESC_PIN")
							+ TrendingModelUtil.getValue("OUTPUT_DOLLARS"),
					incentiveTrendModelManager.predictIncentiveOutput(
							forecastOutputFrame,
							TrendingModelUtil
									.getValue("INCENTIVE_PCT_NONDESC_PIN")
									+ TrendingModelUtil.getValue("OUTPUT")));
			logger.info("IncentiveOutputColumns Map after doller output:."
					+ incentiveOutputColumns);

			// Add incentive output columns to Data frame.
			forecastOutputFrame = trendModelPostProcessor
					.addOutputColumnsToDataFrame(
							forecastOutputFrame.collectAsList(),
							forecastOutputFrame,
							incentiveOutputDollarColumns.entrySet());
		} catch (Exception analyticsRuntimeException) {
			throw new AnalyticsRuntimeException(
					analyticsRuntimeException.getMessage());
		}
		logger.info("Incentive Forecast Model is completed.");
		return forecastOutputFrame;
	}

	/**
	 * We run the dealer gross Model, in this first will do regression on the
	 * history input by using these variables dealer_gross_pct as dependent
	 * variable and years_since_major_change,months_after_launch_nbr,
	 * months_b4_next_launch as independent list. We will measure the intercept
	 * and coefficients for each column. By using the intercept and coefficients
	 * we will forecast the data using future frame and will add calculated
	 * dealer_gross_pct and dealer_gross_amt values to future frame.
	 * 
	 * @param forecastOutputFrame
	 * @param historyDataFrame
	 * @param trendModelsDTO
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private DataFrame runDealerGrossTrend(DataFrame forecastOutputFrame,
			DataFrame historyDataFrame, TrendCalculationsDTO trendModelsDTO)
			throws IOException, AnalyticsRuntimeException {
		// Calling the Dealer Gross Model.
		logger.info("Dealer Gross Forecast Model is started.");
		try {
			forecastOutputFrame = addColumnToDataFrame(
					forecastOutputFrame,
					runDealerGrossModel(historyDataFrame, forecastOutputFrame,
							trendModelsDTO.getDealergrossDependantVar(),
							trendModelsDTO
									.getDealergrossIndependentColumnList()),
					TrendingModelUtil.getValue("DEALER_GROSS_PCT_OUTPUT"));

			// Calculating the dealer_gross value in dollar.
			forecastOutputFrame = addColumnToDataFrame(forecastOutputFrame,
					predictDealerGross(forecastOutputFrame),
					TrendingModelUtil.getValue("DEALER_GROSS_OUTPUT"));
		} catch (AnalyticsRuntimeException analyticsRuntimeException) {
			throw new AnalyticsRuntimeException(
					analyticsRuntimeException.getMessage());
		}
		logger.info("Dealer Gross Forecast Model is completed.");
		return forecastOutputFrame;
	}

	/**
	 * We will calculate the DPH Value based on the previous row of each series
	 * and also calculates the CFTP ,CFTP Pin based on the formula and using
	 * regression. We calculated CFTP like this
	 * (MSRP-increasing_tms_inctv_amt+DPH). For CFTP Pin, will do regression by
	 * using the history frame, cftp_pin_amt as dependent variable and
	 * years_since_major_change,months_after_launch_nbr,new_model_year_flag as
	 * independent list and calculates the CFTP Pin forecast values and finally
	 * calculated DPH, CFTP and CFTP Pin values add to future frame.
	 * 
	 * @param forecastOutputFrame
	 * @param previousDphValue
	 * @param trendModelsDTO
	 * @param historyDataFrame
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private DataFrame runDPHCFTPTrend(DataFrame forecastOutputFrame,
			double previousDphValue, TrendCalculationsDTO trendModelsDTO,
			DataFrame historyDataFrame) throws IOException,
			AnalyticsRuntimeException {
		// Calling the DPH Model.
		logger.info("DPH Forecast Model is started.");
		try {
			forecastOutputFrame = addColumnToDataFrame(
					forecastOutputFrame,
					runDPHModel(forecastOutputFrame, previousDphValue),
					TrendingModelUtil.getValue("DPH")
							+ TrendingModelUtil.getValue("OUTPUT"));
			logger.info("DPH Forecast Model is completed.");

			forecastOutputFrame = calculateCFTPValues(trendModelsDTO,
					forecastOutputFrame, historyDataFrame);
		} catch (AnalyticsRuntimeException analyticsRuntimeException) {
			throw new AnalyticsRuntimeException(
					analyticsRuntimeException.getMessage());
		}
		return forecastOutputFrame;
	}

	/**
	 * We run the MSRPModel, in this first based on the dependent and
	 * independent variables and previous MSRP Value we will start the
	 * calculation. We calculate the average MSRP from the history frame and
	 * then calculates the average MSRP percent over the previous year. Once we
	 * got the MSRP Percent change over previous year, will do regression on the
	 * avg_msrp_amt_chg_pct as dependent and major_change,minor_change,
	 * new_model_year_flag as independent list. We will calculates the intercept
	 * and coefficients for each column back. By using the intercept and
	 * coefficients we will forecast the data using future frame.
	 * 
	 * @param historyDataFrame
	 * @param futureFrame
	 * @param previousMsrpValue
	 * @param dependentVariable
	 * @param independentVariables
	 * @param maxModelYearForPreviousMonth
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private Map<String, List<Double>> runMSRPModel(DataFrame historyDataFrame,
			DataFrame futureFrame, double previousMsrpValue,
			String dependentVariable, List<String> independentVariables,
			int maxModelYear) throws IOException, AnalyticsRuntimeException {

		TrendDataPreparation trendDataPreparation = new TrendDataPreparation();
		DataFrame avgFrame = trendDataPreparation
				.calculateAvgMSRP(historyDataFrame.select(historyDataFrame
						.col(TrendingModelUtil.getValue("SERIES")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")), historyDataFrame
								.col(TrendingModelUtil.getValue("MSRP")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("CHANGE_CATEGORY")), historyDataFrame
								.col(TrendingModelUtil
										.getValue("NEW_MODEL_YEAR_FLAG")),
						historyDataFrame.col(TrendingModelUtil
								.getValue("LATEST_MODEL_YEAR"))));

		// Calculating the percentage over previous year.
		DataFrame pctOverPreviousYear = trendDataPreparation
				.getMsrpPctOverPreviousYear(avgFrame);

		// Calculating the average of MSRP Based on the series_id and
		// model_year and Calculating the MSRP Percent over previous year.
		MSRPTrendModelManager msrpModelManager = new MSRPTrendModelManager();

		// Prepare regression input.
		DataFrame regressionFrame = msrpModelManager
				.getMsrpRegressionInput(pctOverPreviousYear);
		regressionFrame.show();

		return msrpModelManager.calculateMsrpTrend(regressionFrame,
				futureFrame, previousMsrpValue, dependentVariable,
				independentVariables, maxModelYear);
	}

	/**
	 * We run the Incentive Model,in this first will do regression on the
	 * history input by using these variables
	 * tms_inctv_pct,increasing_tms_inctv_pct,pin_inctv_pct,
	 * increasing_pin_inctv_pct as dependent variables and
	 * years_since_major_change,months_after_launch_nbr,months_b4_next_launch as
	 * independent list. We will measure the intercept and coefficients for each
	 * column. By using the intercept and coefficients we will forecast the data
	 * using future frame and will add calculated values to future frame.
	 * 
	 * @param historyDataFrame
	 * @param futureFrame
	 * @param trendCalculationsDTO
	 * @param dependentVariable
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private List<Double> runIncentiveModel(DataFrame historyDataFrame,
			DataFrame futureFrame, TrendCalculationsDTO trendCalculationsDTO,
			String dependentVariable, double previousIncentiveValue)
			throws IOException, AnalyticsRuntimeException {

		// Initializing incentive trend model manager.
		IncentiveTrendModelManager incentiveTrendModelManager = new IncentiveTrendModelManager();
		List<Double> forecastValues;
		DataFrame regFrame;

		// Reading independent columns list.
		List<String> independentList = trendCalculationsDTO
				.getIncentiveIndependentColumnList();
		trendCalculationsDTO.setDependantVar(dependentVariable);
		logger.info("dependentVariable: " + dependentVariable
				+ "Independent Variables List: " + independentList);

		LinearModelDTO linearModelDTO = null;
		TrendModelManager trendModelManager = new TrendModelManager();

		// Here, will do regression on these dependent variables one by one.
		// tms_inctv_pct,pin_inctv_pct
		regFrame = historyDataFrame.select(historyDataFrame
				.col(dependentVariable), historyDataFrame.col(TrendingModelUtil
				.getValue("YEARS_SINCE_MAJOR_CHANGE")), historyDataFrame
				.col(TrendingModelUtil.getValue("MONTHS_AFTER_LAUNCH")),
				historyDataFrame.col(TrendingModelUtil
						.getValue("MONTHS_B4_NEXT_LAUNCH")));

		// Removing same values columns from the data frame.
		linearModelDTO = trendModelManager.callRegressionModel(regFrame,
				dependentVariable, independentList, false);

		// Forecasting data based on the regression values.
		try {
			forecastValues = incentiveTrendModelManager
					.predictIncentiveForecastValues(dependentVariable,
							futureFrame, linearModelDTO,
							previousIncentiveValue, historyDataFrame);
		} catch (Exception analyticsRuntimeException) {
			throw new AnalyticsRuntimeException(
					analyticsRuntimeException.getMessage());
		}
		return forecastValues;
	}

	/**
	 * We run the dealer gross Model, in this first will do regression on the
	 * history input by using these variables dealer_gross_pct as dependent
	 * variable and years_since_major_change,months_after_launch_nbr,
	 * months_b4_next_launch as independent list. We will measure the intercept
	 * and coefficients for each column. By using the intercept and coefficients
	 * we will forecast the data using future frame and will add calculated
	 * dealer_gross_pct and dealer_gross_amt values to future frame.
	 * 
	 * @param regressionInputFrame
	 * @param trendInputFrame
	 * @param dependentVar
	 * @param independentVariables
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public List<Double> runDealerGrossModel(DataFrame regressionInputFrame,
			DataFrame trendInputFrame, String dependentVar,
			List<String> independentVariables) throws IOException,
			AnalyticsRuntimeException {

		// Taking required columns for dealer gross regression.
		DataFrame dealerFrame = regressionInputFrame.select(
				regressionInputFrame.col(TrendingModelUtil
						.getValue("DEALER_GROSS_PCT")), regressionInputFrame
						.col(TrendingModelUtil
								.getValue("YEARS_SINCE_MAJOR_CHANGE")),
				regressionInputFrame.col(TrendingModelUtil
						.getValue("MONTHS_AFTER_LAUNCH")), regressionInputFrame
						.col(TrendingModelUtil
								.getValue("MONTHS_B4_NEXT_LAUNCH")));

		// Initializing the dealer gross model manager and calculates the dealer
		// gross percent.
		DealerGrossTrendModelManager dealerGrossModelManager = new DealerGrossTrendModelManager();
		return dealerGrossModelManager.calculateDealerGrossTrend(dealerFrame,
				trendInputFrame, dependentVar, independentVariables);
	}

	/**
	 * This method runs the DPH Model and gives the forecast values.
	 * 
	 * @param regressionInputFrame
	 * @param trendInputFrame
	 * @param dependentVar
	 * @param independentVariables
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private List<Double> runDPHModel(DataFrame trendInputFrame,
			double previousDphValue) throws IOException,
			AnalyticsRuntimeException {

		// Initializing the model manager.
		DPHTrendModelManager dphModelManager = new DPHTrendModelManager();
		return dphModelManager.calculateDPHTrend(trendInputFrame,
				previousDphValue);
	}

	/**
	 * This method adds the MSRP Forecast values to future frame.
	 * 
	 * @param trendInputFrame
	 * @param msrpForecastValues
	 * @return
	 */
	private DataFrame addColumnToForecastFrame(DataFrame trendInputFrame,
			List<Double> msrpForecastValues, String columnName) {
		List<Row> rows = trendInputFrame.collectAsList();
		List<Row> rowsWithForecastValues = new ArrayList<>(rows.size());

		// Reading the forecast values.
		for (int j = 0; j < rows.size(); j++) {
			List<Object> newRow = new ArrayList<>(
					JavaConversions.asJavaList(rows.get(j).toSeq()));
			newRow.add(msrpForecastValues.get(j));
			rowsWithForecastValues.add(RowFactory.create(newRow
					.toArray(new Object[newRow.size()])));
		}

		// Adding values to input frame.
		List<StructField> structFields = new ArrayList<>(
				JavaConversions.asJavaList(trendInputFrame.schema()));
		structFields.add(new StructField(columnName, DataTypes.DoubleType,
				true, Metadata.empty()));
		StructType structType = new StructType(
				structFields.toArray(new StructField[structFields.size()]));
		trendInputFrame = TrendingModelUtil.getHivecontext().createDataFrame(
				TrendingModelUtil.getJavaSparkContext().parallelize(
						rowsWithForecastValues), structType);
		return trendInputFrame;
	}

	/**
	 * This method calls the regression and provide the intercept and
	 * coefficient values.
	 * 
	 * @param regressionInput
	 * @param dependentVariable
	 * @param independentVariables
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public LinearModelDTO callRegressionModel(DataFrame regressionInput,
			String dependentVariable, List<String> independentVariables,
			boolean interceptValue) {

		// Initializing the linear model information
		LinearModelDTO linearModelDTO = new LinearModelDTO();
		Map<String, Double> values = new HashMap<>();
		logger.info("Regression is started.");
		logger.info("DependantVar: " + dependentVariable
				+ " : IndependentColumnList: " + independentVariables);

		// Calling the regression.
		try {
			LinearModel linearModel = new LinearModel(regressionInput,
					dependentVariable, independentVariables, interceptValue);
			if (independentVariables.size() > 1) {
				List<Double> coefficients = linearModel.getCoefficients();
				linearModelDTO.setCoefficients(coefficients);
				linearModelDTO.setIntercept(coefficients.get(0));
				coefficients.remove(0);

				// Setting column names and coefficient values.
				for (String columnName : independentVariables) {
					int index = independentVariables.indexOf(columnName);
					values.put(columnName, coefficients.get(index));
				}
			} else {
				linearModelDTO.setIntercept(linearModel.getIntercept());
				values.put(independentVariables.get(0),
						linearModel.getCoefficient());
			}
			linearModelDTO.setCoefficientValues(values);
		} catch (Exception exception) {
			logger.error(exception.getMessage());
			linearModelDTO.setIntercept(0.0);
		}
		logger.info("Regression is completed.");
		logger.info("Column and coefficient values: " + values);
		return linearModelDTO;
	}

	/**
	 * This method calculates the CFTP values based on the MSRP, incentive
	 * percent non desc output and dph output.
	 * 
	 * @param futureDataFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	private List<Double> calculateCFTPValues(DataFrame futureDataFrame)
			throws AnalyticsRuntimeException {

		// Calculates CFTP.
		DataFrame cftpFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("MSRP")), futureDataFrame
				.col(TrendingModelUtil
						.getValue("INCENTIVE_PCT_NONDESC_OUTPUT_DOLLERS")),
				futureDataFrame.col(TrendingModelUtil.getValue("DPH_OUTPUT")));

		// Reading the data as RDD and do CFTP values calculation.
		JavaRDD<Double> rddValues = cftpFrame.toJavaRDD().map(
				new Function<Row, Double>() {
					private static final long serialVersionUID = -4052182812916729718L;

					@Override
					public Double call(Row row) {
						double msrp = row.isNullAt(0) ? 0.0 : row.getDouble(0);
						double incentivePctNondesc = row.isNullAt(1) ? 0.0
								: row.getDouble(1);
						double dph = row.isNullAt(2) ? 0.0 : row.getDouble(2);
						return (double) Math
								.round((msrp - incentivePctNondesc + dph) / 50) * 50;
					}
				});
		logger.info("ForecastCFTPValues: " + rddValues.collect());
		return rddValues.collect();
	}

	/**
	 * This method calculates the CFTP values based on the MSRP, incentive
	 * percent non desc output and dph output.
	 * 
	 * @param futureDataFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	private List<Double> calculateCFTPPinValues(DataFrame futureDataFrame)
			throws AnalyticsRuntimeException {

		// Calculates CFTP.
		DataFrame cftpFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("MSRP")), futureDataFrame
				.col(TrendingModelUtil
						.getValue("INCENTIVE_PCT_PIN_OUTPUT_DOLLERS")),
				futureDataFrame.col(TrendingModelUtil.getValue("DPH_OUTPUT")));

		// Reading the data as RDD and do CFTP values calculation.
		JavaRDD<Double> rddValues = cftpFrame.toJavaRDD().map(
				new Function<Row, Double>() {
					private static final long serialVersionUID = -4052182812916729718L;

					@Override
					public Double call(Row row) {
						double msrpAmount = row.isNullAt(0) ? 0.0 : row
								.getDouble(0);
						double pinIncentiveAmount = row.isNullAt(1) ? 0.0 : row
								.getDouble(1);
						double dph = row.isNullAt(2) ? 0.0 : row.getDouble(2);
						return (double) Math.round((msrpAmount
								- pinIncentiveAmount + dph) / 50) * 50;
					}
				});
		logger.info("ForecastCFTPPinValues: " + rddValues.collect());
		return rddValues.collect();
	}

	/**
	 * This method adds the MSRP Forecast values to input frame.
	 * 
	 * @param trendInputFrame
	 * @param msrpForecastValues
	 * @return
	 */
	public DataFrame addColumnToDataFrame(DataFrame trendInputFrame,
			List<Double> msrpForecastValues, String dependentVariable) {
		List<Row> rows = trendInputFrame.collectAsList();
		List<Row> rowsWithForecastValues = new ArrayList<>(rows.size());

		// Reading the forecast values.
		for (int j = 0; j < rows.size(); j++) {
			List<Object> newRow = new ArrayList<>(
					JavaConversions.asJavaList(rows.get(j).toSeq()));
			newRow.add(msrpForecastValues.get(j));
			rowsWithForecastValues.add(RowFactory.create(newRow
					.toArray(new Object[newRow.size()])));
		}

		// Adding values to input frame.
		JavaRDD<Row> rowJavaRDD = TrendingModelUtil.getJavaSparkContext()
				.parallelize(rowsWithForecastValues);
		List<StructField> structFields = new ArrayList<>(
				JavaConversions.asJavaList(trendInputFrame.schema()));
		structFields.add(new StructField(dependentVariable,
				DataTypes.DoubleType, true, Metadata.empty()));
		StructType structType = new StructType(
				structFields.toArray(new StructField[structFields.size()]));
		trendInputFrame = TrendingModelUtil.getHivecontext().createDataFrame(
				rowJavaRDD, structType);
		return trendInputFrame;
	}

	/**
	 * This method reads MSRP and dealer gross percent output and calculates the
	 * dealer gross amount in dollars. This will calculates based on the
	 * multiplication of MSRP and dealer gross percent.
	 * 
	 * @param futureDataFrame
	 * @return
	 */
	private List<Double> predictDealerGross(DataFrame futureDataFrame) {
		// Collect data from the data frame.
		List<Double> rows = new ArrayList<>();

		// Read the MSRP and the Dealer_Margin_Pct values.
		DataFrame dealerMarginFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("MSRP")), futureDataFrame
				.col(TrendingModelUtil.getValue("DEALER_GROSS_PCT_OUTPUT")));

		// Collecting the data and calculating the dealer margin value.
		for (Row row : dealerMarginFrame.collect()) {
			double dealerGross = row.getDouble(0) * row.getDouble(1);
			rows.add((double) Math.round(dealerGross / 50) * 50);
		}
		return rows;
	}

	/**
	 * This method calculates CFTP and CFTP Pin values as per the formula.
	 * 
	 * @param trendCalculationsDTO
	 * @param forecastOutputFrame
	 * @param historyDataFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	private DataFrame calculateCFTPValues(
			TrendCalculationsDTO trendCalculationsDTO,
			DataFrame forecastOutputFrame, DataFrame historyDataFrame)
			throws AnalyticsRuntimeException {
		// Calculations of CFTP and CFTP Pin.
		logger.info("CFTP Calculation started.");

		// Calculating CFTP and adding to output frame.
		forecastOutputFrame = addColumnToDataFrame(forecastOutputFrame,
				calculateCFTPValues(forecastOutputFrame),
				TrendingModelUtil.getValue("CFTP"));

		// Calculate CFTP_PIN and adding to output frame.
		forecastOutputFrame = addColumnToDataFrame(
				forecastOutputFrame,
				calculateCFTPPinValues(forecastOutputFrame),
				TrendingModelUtil.getValue("CFTP_PIN")
						+ TrendingModelUtil.getValue("OUTPUT"));
		logger.info("CFTP Calculation ended.");
		return forecastOutputFrame;
	}

	/**
	 * This method saves the forecast output data to hive output table.
	 * 
	 * @param forecastOutputFrame
	 * @param forecastMap
	 * @throws AnalyticsRuntimeException
	 */
	private void saveForecastOutputData(DataFrame forecastOutputFrame,
			Map<String, List<Double>> forecastMap, DataFrame historyOutputFrame)
			throws AnalyticsRuntimeException {
		logger.info("saveForecastOutputData start.");
		// Collect the output and save into output tables for all the models.
		logger.info("Saving all Forecast Models data to output table is started.");
		if (forecastMap.isEmpty()) {
			logger.error(TrendingModelUtil.getValue("NO_FORECAST_VALUES"));
			throw new AnalyticsRuntimeException(
					TrendingModelUtil.getValue("NO_FORECAST_VALUES"));
		} else {
			// Saving the forecast incentive Data to hive table.
			TrendModelPostProcessor modelPostProcessor = new TrendModelPostProcessor();
			modelPostProcessor.saveForecastData(forecastOutputFrame,
					forecastMap, historyOutputFrame);
			logger.info("Saving all Forecast Models data to output table is completed.");
		}
		logger.info("saveForecastOutputData end.");
	}

	/**
	 * This method calculates the latest model year values.
	 * 
	 * @param inputFrame
	 * @param previousMonthModelYear
	 * @return
	 */
	public List<Integer> calculateLatestModelYear(DataFrame inputFrame) {
		logger.info("calculateLatestModelYear start.");
		// Reading the max model year and change category values.
		DataFrame latestModelYearFrame = inputFrame.select(
				inputFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")),
				inputFrame.col(TrendingModelUtil.getValue("BUSINESS_MONTH")),
				inputFrame.col(TrendingModelUtil.getValue("LAUNCH_MONTH_IND")));

		// Taking the max model year list.
		List<Integer> latestModelYearList = new ArrayList<>();

		// Getting max model year list.
		Map<String, Integer> maxModelYearList = maxModelYearList(latestModelYearFrame);

		// Calculating the latest model based on the previous latest model year.
		String previousMonth = "";
		for (Row row : latestModelYearFrame.collect()) {

			// Reading each business month.
			String businessMonth = row.isNullAt(1) ? "" : row.getString(1);

			// Calculating the old month latest model year.
			int launchMonthIndicator = row.isNullAt(2) ? 0 : row.getInt(2);
			int latestModelYearForPreviousMonth = maxModelYearList
					.containsKey(previousMonth) ? maxModelYearList
					.get(previousMonth) : 0;
			if (launchMonthIndicator > 0) {
				int latestModelYear = latestModelYearForPreviousMonth
						+ launchMonthIndicator;

				logger.info("latestModelYearForPreviousMonth:."
						+ latestModelYearForPreviousMonth
						+ " : latestModelYear: " + latestModelYear + " : "
						+ launchMonthIndicator + " : previousMonth: "
						+ previousMonth + " : Current businessMonth: "
						+ businessMonth);

				previousMonth = businessMonth;
				latestModelYearList.add(latestModelYear);
			} else {
				latestModelYearList.add(latestModelYearForPreviousMonth);
			}
		}
		logger.info("calculateLatestModelYear end.");
		return latestModelYearList;
	}

	/**
	 * This method calculates the sell down index based on the months after
	 * launch , model year and latest model year.
	 * 
	 * @param inputFrame
	 * @return
	 */
	public List<Integer> calculateSellDownIndex(DataFrame inputFrame) {
		logger.info("calculateSellDownIndex start.");
		// Reading the months after launch , model year and latest model year.
		DataFrame sellDownFrame = inputFrame
				.select(inputFrame.col(TrendingModelUtil
						.getValue("MONTHS_AFTER_LAUNCH")), inputFrame
						.col(TrendingModelUtil.getValue("MODEL_YEAR")),
						inputFrame.col(TrendingModelUtil
								.getValue("LATEST_MODEL_YEAR")));

		// Creating the sell indexes list and calculating the values.
		List<Integer> sellIndexesList = new ArrayList<>();
		for (Row row : sellDownFrame.collect()) {
			int monthsAfterLaunch = row.isNullAt(0) ? 0 : row.getInt(0);
			int modelYear = row.isNullAt(1) ? 0 : row.getInt(1);
			int latestModelYear = row.isNullAt(2) ? 0 : row.getInt(2);
			sellIndexesList
					.add(modelYear < latestModelYear ? (monthsAfterLaunch < 4 ? 1
							: 0)
							: 0);
		}
		logger.info("calculateSellDownIndex end.");
		return sellIndexesList;
	}

	/**
	 * This method calculates the years_since_major_change,
	 * years_since_minor_change,months_after_launch, months_b4_next_launch and
	 * latest_model_year values , adds to input frame for doing the regression
	 * and forecasting the values.
	 * 
	 * @param inputFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public Map<String, List<Integer>> addColumnValuesToFrame(
			DataFrame inputFrame) throws AnalyticsRuntimeException {
		logger.info("addCalculatedColumnValuesToFrame start.");
		// Initializing the column values.
		Map<String, List<Integer>> forecastMap = new HashMap<>();

		// Adding the segment non fleet percent.
		forecastMap.put(TrendingModelUtil.getValue("MONTHS_AFTER_LAUNCH"),
				calculateMonthsAfterLaunch(inputFrame));

		// Adding the competitor segment share.
		forecastMap.put(TrendingModelUtil.getValue("MONTHS_B4_NEXT_LAUNCH"),
				calculateMonthsBeforeNextLaunch(inputFrame));

		// Adding dealer margin details.
		forecastMap.put(TrendingModelUtil.getValue("LATEST_MODEL_YEAR_OUTPUT"),
				calculateLatestModelYear(inputFrame));

		logger.info("addCalculatedColumnValuesToFrame end.");
		return forecastMap;
	}

	/**
	 * This method adds the sell down index to input frame once calculates.
	 * 
	 * @param regressionFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public Map<String, List<Integer>> addSellDownIndColumnToFrame(
			DataFrame regressionFrame) throws AnalyticsRuntimeException {
		logger.info("addSellDownIndColumnToFrame start.");
		// Initializing the column values.
		Map<String, List<Integer>> forecastMap = new HashMap<>();

		// Adding the segment non fleet percent.
		forecastMap.put(TrendingModelUtil.getValue("SELL_DOWN_IND"),
				calculateSellDownIndex(regressionFrame));

		logger.info("addSellDownIndColumnToFrame end.");
		return forecastMap;
	}

	/**
	 * This method calculates the new model year flag and result write to future
	 * frame.
	 * 
	 * @param futureFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public Map<String, List<Integer>> addNewModelYearFlagToFutureFrame(
			DataFrame futureFrame) throws AnalyticsRuntimeException {
		logger.info("addNewModelYearFlagToFutureFrame start.");
		// Initializing the column values.
		Map<String, List<Integer>> forecastMap = new HashMap<>();

		// Adding the segment non fleet percent.
		forecastMap.put(TrendingModelUtil.getValue("NEW_MODEL_YEAR_FLAG"),
				calculateNewModelYearFlag(futureFrame));

		logger.info("addNewModelYearFlagToFutureFrame end.");
		return forecastMap;
	}

	/**
	 * This method adds the columns to data frame.
	 * 
	 * @param rows
	 * @param trendInputFrame
	 * @param values
	 * @return
	 */
	public DataFrame addOutputColumnsToDataFrame(List<Row> rows,
			DataFrame trendInputFrame,
			Set<Map.Entry<String, List<Integer>>> values) {
		logger.info("addOutputColumnsToDataFrame start");
		if (null != rows && !rows.isEmpty()) {
			List<Row> rowsWithForecastValues = new ArrayList<>(rows.size());

			// Reading the forecast values.
			for (int j = 0; j < rows.size(); j++) {
				List<Object> newRow = new ArrayList<>(
						JavaConversions.asJavaList(rows.get(j).toSeq()));
				for (Map.Entry<String, List<Integer>> entry : values) {
					List<Integer> listValues = entry.getValue();
					if (!listValues.isEmpty())
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
			for (Map.Entry<String, List<Integer>> entry : values) {
				String columnName = entry.getKey();
				structFields.add(new StructField(columnName,
						DataTypes.IntegerType, true, Metadata.empty()));
			}

			// Creating the data frame based on the RDD and struct type.
			StructType structType = new StructType(
					structFields.toArray(new StructField[structFields.size()]));
			trendInputFrame = TrendingModelUtil.getHivecontext()
					.createDataFrame(rowJavaRDD, structType);
			logger.info("addOutputColumnsToDataFrame end");
		}
		return trendInputFrame;
	}

	/**
	 * This method returns the max model years for each business month.
	 * 
	 * @param forecastFrame
	 * @return
	 */
	public Map<String, Integer> maxModelYearList(DataFrame forecastFrame) {
		logger.info("maxModelYearList start.");
		Map<String, Integer> maxModelYearMap = new HashMap<>();
		Row[] row = forecastFrame
				.groupBy(TrendingModelUtil.getValue("BUSINESS_MONTH"))
				.agg(forecastFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")),
						max(
								forecastFrame.col(TrendingModelUtil
										.getValue("MODEL_YEAR"))).as(
								TrendingModelUtil.getValue("MAX_MODEL_YEAR")))
				.collect();
		for (Row rowvalue : row) {
			String businessMonth = rowvalue.isNullAt(0) ? "" : rowvalue
					.getString(0);
			int maxModelYear = rowvalue.isNullAt(1) ? 0 : rowvalue.getInt(1);
			maxModelYearMap.put(businessMonth, maxModelYear);
		}
		logger.info(maxModelYearMap);
		logger.info("maxModelYearList end.");
		return maxModelYearMap;
	}

	/**
	 * This method calls the regression without intercept and provide the
	 * coefficient values.
	 * 
	 * @param regressionInput
	 * @param dependentVariable
	 * @param independentVariables
	 * @param interceptValue
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public LinearModelDTO callRegressionModelWithNoIntercept(
			DataFrame regressionInput, String dependentVariable,
			List<String> independentVariables, boolean interceptValue)
			throws AnalyticsRuntimeException {
		logger.info("callRegressionModelWithNoIntercept start.");
		// Initializing the linear model information
		LinearModelDTO linearModelDTO = new LinearModelDTO();
		Map<String, Double> values = new HashMap<>();

		try {
			// Calling the regression.
			LinearModel linearModel = new LinearModel(regressionInput,
					dependentVariable, independentVariables, interceptValue);
			if (independentVariables.size() > 1) {
				List<Double> coefficients = linearModel.getCoefficients();
				linearModelDTO.setCoefficients(coefficients);
				linearModelDTO.setIntercept(coefficients.get(0));
				// Setting column names and coefficient values.
				for (String columnName : independentVariables) {
					int index = independentVariables.indexOf(columnName);
					values.put(columnName, coefficients.get(index));
				}
			} else {
				linearModelDTO.setIntercept(linearModel.getIntercept());
				values.put(independentVariables.get(0),
						linearModel.getCoefficient());
			}
			linearModelDTO.setCoefficientValues(values);
		} catch (Exception exception) {
			logger.error(exception.getMessage());
			linearModelDTO.setIntercept(0.0);
		}
		logger.info("Column and its coefficient: " + values);
		logger.info("callRegressionModelWithNoIntercept end.");
		return linearModelDTO;
	}

	/**
	 * This method calculates the new model year flag based on the model_year
	 * and latest_model_year. If model year and latest model year is equal then
	 * 1 else 0.
	 * 
	 * @param futureFrame
	 * @return
	 */
	public List<Integer> calculateNewModelYearFlag(DataFrame futureFrame) {
		logger.info("calculateNewModelYearFlag start.");
		// Reading the model year and latest model year.
		DataFrame yearsFrameData = futureFrame.select(futureFrame
				.col(TrendingModelUtil.getValue("MODEL_YEAR")), futureFrame
				.col(TrendingModelUtil.getValue("LATEST_MODEL_YEAR_OUTPUT")));
		yearsFrameData.show(332);

		// This loops the all the data and calculates the new model year flag.
		List<Integer> newModelYearFlagList = new ArrayList<>();
		for (Row row : yearsFrameData.collect()) {
			int modelYear = row.isNullAt(0) ? 0 : row.getInt(0);
			int latestModelYear = row.isNullAt(1) ? 0 : row.getInt(1);
			int newModelYearFlag = modelYear == latestModelYear ? 1 : 0;
			newModelYearFlagList.add(newModelYearFlag);
		}
		logger.info("calculateNewModelYearFlag end.");
		return newModelYearFlagList;
	}

	/**
	 * This method gives the max_business_month based on the lauch_month_ind
	 * value.
	 * 
	 * @param modelInputFrame
	 * @return
	 */
	public Map<Integer, String> getMaxBusinessMonthForModelYear(
			DataFrame modelInputFrame) {
		logger.info("getMaxBusinessMonthForModelYear start.");
		Map<Integer, String> maxBusinessMonthListByYear = new HashMap<>();
		// Reading the model year and latest model year.
		DataFrame yearsFrameData = modelInputFrame
				.groupBy(
						modelInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")),
						modelInputFrame.col(TrendingModelUtil
								.getValue("LAUNCH_MONTH_IND")))
				.agg(max(modelInputFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH"))),
						modelInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")),
						modelInputFrame.col(TrendingModelUtil
								.getValue("LAUNCH_MONTH_IND")))
				.filter(modelInputFrame.col(
						TrendingModelUtil.getValue("LAUNCH_MONTH_IND"))
						.equalTo(
								TrendingModelUtil
										.getValue("LAUNCH_MONTH_IND_VALUE")));
		yearsFrameData.show();

		// This method collects the business_month and model_year and put into
		// map.
		for (Row row : yearsFrameData.collect()) {
			String businessMonth = row.getString(0);
			int modelYear = row.isNullAt(1) ? 0 : row.getInt(1);
			maxBusinessMonthListByYear.put(modelYear, businessMonth);
		}
		logger.info("maxBusinessMonthListByYear: " + maxBusinessMonthListByYear);
		logger.info("getMaxBusinessMonthForModelYear end.");
		return maxBusinessMonthListByYear;
	}

	/**
	 * This method gives reads the change category values and model_year values
	 * based on the change_category filter.
	 * 
	 * @param modelInputFrame
	 * @return
	 */
	public Set<Integer> getMaxModelYearForChangeCategory(
			DataFrame modelInputFrame, String categoryFilter) {
		logger.info("getMaxModelYearForChangeCategory start.");
		Set<Integer> maxModelYearListByChangeCategory = new HashSet<>();
		// Reading the model year and latest model year.
		DataFrame maxModelYearData = modelInputFrame
				.select(modelInputFrame.col(TrendingModelUtil
						.getValue("CHANGE_CATEGORY")),
						modelInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")))
				.filter(modelInputFrame.col(
						TrendingModelUtil.getValue("CHANGE_CATEGORY")).equalTo(
						categoryFilter)).distinct();

		// Collecting the model_year list by change category filter.
		for (Row row : maxModelYearData.collect()) {
			int modelYear = row.isNullAt(1) ? 0 : row.getInt(1);
			maxModelYearListByChangeCategory.add(modelYear);
		}
		logger.info("maxModelYearListByChangeCategory: "
				+ maxModelYearListByChangeCategory);
		logger.info("getMaxModelYearForChangeCategory end.");
		return maxModelYearListByChangeCategory;
	}

	/**
	 * This calculates the average model year sales percent.
	 * 
	 * @param historyInputFrame
	 * @return
	 */
	public Map<Integer, Double> calculateModelYearSalesPct(
			DataFrame historyInputFrame) {
		logger.info("calculateModelYearSalesPct start.");

		// Reading the first 5 months average model year sales percent.
		Map<Integer, Double> monthsValues = new HashMap<>();

		// Reading the model year and latest model year.
		DataFrame modelYearSalesPctData = historyInputFrame
				.groupBy(
						historyInputFrame.col(TrendingModelUtil
								.getValue("MONTHS_AFTER_LAUNCH")),
						historyInputFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")),
						historyInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")))
				.agg(avg(historyInputFrame.col(TrendingModelUtil
						.getValue("MODEL_YEAR_SALES_SPLIT"))),
						historyInputFrame.col(TrendingModelUtil
								.getValue("MONTHS_AFTER_LAUNCH")),
						historyInputFrame.col(TrendingModelUtil
								.getValue("NEW_MODEL_YEAR_FLAG")),
						historyInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")))
				.filter("new_model_year_flag = 1 and months_after_launch_nbr between 1 and 5  and model_year > (model_year-5)");

		// Collecting the model_year list by change category filter.
		for (Row row : modelYearSalesPctData.collect()) {
			double averageSalesPct = row.isNullAt(0) ? 0 : row.getDouble(0);
			int monthsAfterLaunch = row.isNullAt(1) ? 0 : row.getInt(1);
			monthsValues.put(monthsAfterLaunch, averageSalesPct);
		}
		logger.info("monthsValues: " + monthsValues);
		logger.info("calculateModelYearSalesPct end.");
		return monthsValues;
	}

	/**
	 * This calculates the average model year sales percent.
	 * 
	 * @param futureFrame
	 * @return
	 */
	public List<Double> forecastModelYearSalesPct(DataFrame futureFrame,
			Map<Integer, Double> averageValues) {
		logger.info("forecastModelYearSalesPct start.");

		List<Double> listOfModelYearSalesPct = new ArrayList<>();

		// Reading the model year and latest model year.
		DataFrame modelYearSalesPctData = futureFrame.select(futureFrame
				.col(TrendingModelUtil.getValue("NEW_MODEL_YEAR_FLAG")),
				futureFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")),
				futureFrame.col(TrendingModelUtil.getValue("BUSINESS_MONTH")),
				futureFrame.col(TrendingModelUtil
						.getValue("MONTHS_AFTER_LAUNCH")));

		// Creating the Map for the business month and model year.
		Map<String, Integer> businessMonthModelYears = new HashMap<>();

		for (Row row : modelYearSalesPctData.collect()) {
			int newModelYearFlag = row.isNullAt(0) ? 0 : row.getInt(0);
			int modelYear = row.isNullAt(1) ? 0 : row.getInt(1);
			String businessMonth = row.isNullAt(2) ? "" : row.getString(2);
			int monthsAfterLaunch = row.isNullAt(3) ? 0 : row.getInt(3);

			if ((newModelYearFlag == 1 && monthsAfterLaunch == 1)
					|| (newModelYearFlag == 1 && monthsAfterLaunch == 2)
					|| (newModelYearFlag == 1 && monthsAfterLaunch == 3)
					|| (newModelYearFlag == 1 && monthsAfterLaunch == 4)
					|| (newModelYearFlag == 1 && monthsAfterLaunch == 5)) {
				businessMonthModelYears.put(businessMonth, modelYear);
			}
		}

		logger.info("businessMonthModelYears: " + businessMonthModelYears);

		// Forecast data by business_month.
		for (Row row : modelYearSalesPctData.collect()) {
			double salesPct = 0.0;

			// Reading the required row data.
			int modelYear = row.isNullAt(1) ? 0 : row.getInt(1);
			String businessMonth = row.isNullAt(2) ? "" : row.getString(2);
			int monthsAfterLaunch = row.isNullAt(3) ? 0 : row.getInt(3);
			logger.info("modelYear: " + modelYear + " businessMonth:"
					+ businessMonth + " : monthsAfterLaunch: "
					+ monthsAfterLaunch);

			// Checking the new model year.
			if (businessMonthModelYears.containsKey(businessMonth)) {

				// Get the model year for the business Month.
				int newModelYear = businessMonthModelYears.get(businessMonth);

				// Checking the model year is old model year or new model year.
				if (newModelYear == modelYear) {
					switch (monthsAfterLaunch) {
					case 1:
						salesPct = averageValues.containsKey(monthsAfterLaunch) ? averageValues
								.get(monthsAfterLaunch) : 0.0;
						listOfModelYearSalesPct.add(salesPct);
						break;
					case 2:
						salesPct = averageValues.containsKey(monthsAfterLaunch) ? averageValues
								.get(monthsAfterLaunch) : 0.0;
						listOfModelYearSalesPct.add(salesPct);
						break;
					case 3:
						salesPct = averageValues.containsKey(monthsAfterLaunch) ? averageValues
								.get(monthsAfterLaunch) : 0.0;
						listOfModelYearSalesPct.add(salesPct);
						break;
					case 4:
						salesPct = averageValues.containsKey(monthsAfterLaunch) ? averageValues
								.get(monthsAfterLaunch) : 0.0;
						listOfModelYearSalesPct.add(salesPct);
						break;
					case 5:
						salesPct = averageValues.containsKey(monthsAfterLaunch) ? averageValues
								.get(monthsAfterLaunch) : 0.0;
						listOfModelYearSalesPct.add(salesPct);
						break;
					default:
						listOfModelYearSalesPct.add(1.0);
						break;
					}
				} else {
					switch (monthsAfterLaunch) {
					case 1:
						salesPct = averageValues.containsKey(1) ? averageValues
								.get(1) : 0.0;
						listOfModelYearSalesPct.add(1 - salesPct);
						break;
					case 2:
						salesPct = averageValues.containsKey(2) ? averageValues
								.get(2) : 0.0;
						listOfModelYearSalesPct.add(1 - salesPct);
						break;
					case 3:
						salesPct = averageValues.containsKey(3) ? averageValues
								.get(3) : 0.0;
						listOfModelYearSalesPct.add(1 - salesPct);
						break;
					case 4:
						salesPct = averageValues.containsKey(4) ? averageValues
								.get(4) : 0.0;
						listOfModelYearSalesPct.add(1 - salesPct);
						break;
					case 5:
						salesPct = averageValues.containsKey(5) ? averageValues
								.get(5) : 0.0;
						listOfModelYearSalesPct.add(1 - salesPct);
						break;
					default:
						listOfModelYearSalesPct.add(0.0);
						break;
					}
				}
			} else {
				listOfModelYearSalesPct.add(0.0);
			}
		}
		logger.info("listOfModelYearSalesPct: " + listOfModelYearSalesPct);
		logger.info("forecastModelYearSalesPct end.");
		return listOfModelYearSalesPct;
	}

	/**
	 * 
	 * @param historyInputFrame
	 * @return
	 */
	public Map<Integer, String> calculateLaunchMonthIndicator(
			DataFrame historyInputFrame) {
		logger.info("calculateLaunchMonthIndicator start.");

		// Reading the first 5 months average model year sales percent.
		Map<Integer, String> launchMonthLists = new HashMap<>();

		// Reading the model year and latest model year.
		DataFrame launchMonthIndictorFrame = historyInputFrame
				.select(historyInputFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")),
						historyInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR"))).sort(
						historyInputFrame.col(TrendingModelUtil
								.getValue("MODEL_YEAR")));
		launchMonthIndictorFrame.show();

		// Collecting the model_year list by change category filter.
		int previousModelYear = 0;
		for (Row row : launchMonthIndictorFrame.collect()) {
			String businessMonth = row.isNullAt(0) ? "" : row.getString(0);
			int modelYear = row.isNullAt(1) ? 0 : row.getInt(1);

			// Checking the previousModelYear
			if (previousModelYear == 0) {
				previousModelYear = modelYear;
			} else if (previousModelYear != modelYear) {
				launchMonthLists.put(modelYear, businessMonth);
				previousModelYear = modelYear;
			}
		}
		logger.info("monthsValues: " + launchMonthLists);
		logger.info("calculateLaunchMonthIndicator end.");
		return launchMonthLists;
	}

	/**
	 * 
	 * @param futureFrame
	 * @param averageValues
	 * @return
	 */
	public List<Integer> forecastLaunchMonthIndicator(DataFrame futureFrame) {
		logger.info("calculateModelYearSalesPct start.");

		List<Integer> launchMonthList = new ArrayList<>();

		// Reading the model year and latest model year.
		DataFrame modelYearSalesPctData = futureFrame.select(
				futureFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")),
				futureFrame.col(TrendingModelUtil.getValue("BUSINESS_MONTH")));

		Map<Integer, String> launchMap = calculateLaunchMonthIndicator(modelYearSalesPctData);
		logger.info("launchMap:" + launchMap);

		for (Row row : modelYearSalesPctData.collect()) {
			int modelYear = row.isNullAt(0) ? 0 : row.getInt(0);
			String businessMonth = row.isNullAt(1) ? "" : row.getString(1);

			// Get business month of particular model year.
			if (launchMap.containsKey(modelYear)) {
				String launchMonth = launchMap.get(modelYear);
				if (launchMonth.equalsIgnoreCase(businessMonth)) {
					launchMonthList.add(1);
				} else {
					launchMonthList.add(0);
				}
			} else {
				launchMonthList.add(0);
			}
		}
		logger.info("launchMonthList: " + launchMonthList);
		logger.info("calculateModelYearSalesPct end.");
		return launchMonthList;
	}

	/**
	 * This method calculates the months_after_launch column values based on the
	 * business month which is less than or equal to current row business month
	 * and launch_month_ind value.
	 * 
	 * @param inputFrame
	 * @return
	 */
	public List<Integer> calculateMonthsAfterLaunch(DataFrame inputFrame) {

		logger.info("calculateMonthsAfterLaunch start.");
		// Reading the max model year and change category values.
		DataFrame monthsAfterLaunchFrame = inputFrame.select(
				inputFrame.col(TrendingModelUtil.getValue("BUSINESS_MONTH")),
				inputFrame.col(TrendingModelUtil.getValue("LAUNCH_MONTH_IND")),
				inputFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")));
		List<Integer> monthsAfterLaunchList = new ArrayList<>();
		Map<Integer, String> maxBusinessMonthListByYear = getMaxBusinessMonthForModelYear(monthsAfterLaunchFrame);
		for (Row row : monthsAfterLaunchFrame.collect()) {

			// Get the current row business month.
			String currentRowBusinessMonth = row.getString(0);

			// Get the current row model year.
			int modelYearValue = row.isNullAt(2) ? 0 : row.getInt(2);

			// Get the max business month based on the model year.
			String maxbusinessMonth = maxBusinessMonthListByYear
					.containsKey(modelYearValue) ? maxBusinessMonthListByYear
					.get(modelYearValue) : null;

			// Take the difference with business month and max business month.
			if (null != currentRowBusinessMonth && null != maxbusinessMonth) {
				String businessMonthYear = currentRowBusinessMonth.substring(0,
						4);
				String businessMonthValue = currentRowBusinessMonth
						.substring(4);
				String maxbusinessMonthYear = maxbusinessMonth.substring(0, 4);
				String maxbusinessMonthValue = maxbusinessMonth.substring(4);
				logger.info("businessMonthYear:" + businessMonthYear
						+ " : businessMonthValue: " + businessMonthValue
						+ " : maxbusinessMonthYear: " + maxbusinessMonthYear
						+ " : maxbusinessMonthValue: " + maxbusinessMonthValue);
				int monthsAfterLaunchValue;
				if (businessMonthYear.equals(maxbusinessMonthYear)) {
					monthsAfterLaunchValue = (Integer
							.parseInt(businessMonthValue) - Integer
							.parseInt(maxbusinessMonthValue));
					if (monthsAfterLaunchValue < 0) {
						monthsAfterLaunchValue = -1 * monthsAfterLaunchValue;
					} else {
						monthsAfterLaunchValue = 1 + monthsAfterLaunchValue;
					}
				} else {
					int yearDifference = Integer.parseInt(businessMonthYear)
							- Integer.parseInt(maxbusinessMonthYear);
					logger.info("yearDifference:" + yearDifference);
					monthsAfterLaunchValue = (yearDifference * 12)
							+ (Integer.parseInt(businessMonthValue) - Integer
									.parseInt(maxbusinessMonthValue)) + 1;
				}
				logger.info("monthsAfterLaunchValue:" + monthsAfterLaunchValue);
				monthsAfterLaunchList.add(monthsAfterLaunchValue);
			} else {
				monthsAfterLaunchList.add(0);
			}
		}
		logger.info("calculateMonthsAfterLaunch end.");
		return monthsAfterLaunchList;
	}

	/**
	 * 
	 * This method calculates the months_b4_next_launch column values based on
	 * the business month which is greater than or equal to current row business
	 * month and launch_month_ind value.
	 * 
	 * @param regressionFrame
	 * @return
	 */
	public List<Integer> calculateMonthsBeforeNextLaunch(
			DataFrame regressionFrame) {

		logger.info("calculateMonthsBeforeNextLaunch start.");
		// Reading the max model year and change category values.
		DataFrame monthsBeforeNextLaunchFrame = regressionFrame.select(
				regressionFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")), regressionFrame
						.col(TrendingModelUtil.getValue("LAUNCH_MONTH_IND")),
				regressionFrame.col(TrendingModelUtil.getValue("MODEL_YEAR")));

		List<Integer> monthsBeforeNextLaunchList = new ArrayList<>();
		final Map<Integer, String> maxBusinessMonthListByYear = getMaxBusinessMonthForModelYear(monthsBeforeNextLaunchFrame);
		for (Row row : monthsBeforeNextLaunchFrame.collect()) {

			// Get the current row business month.
			String currentRowBusinessMonth = row.getString(0);

			// Get the current row model year.
			int modelYearValue = row.isNullAt(2) ? 0 : row.getInt(2);

			// Get the max business month based on the model year.
			String minbusinessMonth = maxBusinessMonthListByYear
					.containsKey(modelYearValue + 1) ? maxBusinessMonthListByYear
					.get(modelYearValue + 1) : null;
			if (null != minbusinessMonth) {
				if (Integer.parseInt(currentRowBusinessMonth) > Integer
						.parseInt(minbusinessMonth)) {
					minbusinessMonth = maxBusinessMonthListByYear
							.containsKey(modelYearValue + 2) ? maxBusinessMonthListByYear
							.get(modelYearValue + 2) : null;
				}
			}

			if (null != currentRowBusinessMonth && null != minbusinessMonth) {
				String businessMonthYear = currentRowBusinessMonth.substring(0,
						4);
				String businessMonthValue = currentRowBusinessMonth
						.substring(4);
				String minbusinessMonthYear = minbusinessMonth.substring(0, 4);
				String minbusinessMonthValue = minbusinessMonth.substring(4);
				int monthsBeforeNextLaunch;
				if (businessMonthYear.equals(minbusinessMonthYear)) {
					monthsBeforeNextLaunch = (Integer
							.parseInt(minbusinessMonthValue) - Integer
							.parseInt(businessMonthValue));
					if (monthsBeforeNextLaunch < 0) {
						monthsBeforeNextLaunch = -1 * monthsBeforeNextLaunch;
					}
				} else {
					int yearDifference = Integer.parseInt(minbusinessMonthYear)
							- Integer.parseInt(businessMonthYear);
					monthsBeforeNextLaunch = (yearDifference * 12)
							+ (Integer.parseInt(minbusinessMonthValue) - Integer
									.parseInt(businessMonthValue));
				}
				monthsBeforeNextLaunchList.add(monthsBeforeNextLaunch + 1);
			} else {
				monthsBeforeNextLaunchList.add(0);
			}
		}
		logger.info("calculateMonthsBeforeNextLaunch end.");
		return monthsBeforeNextLaunchList;
	}

	/**
	 * 
	 * @param seriesFrameData
	 * @param averageValues
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public DataFrame removeUncessaryRows(DataFrame seriesFrameData)
			throws AnalyticsRuntimeException {
		logger.info("removeUncessaryRows start.");

		List<Integer> launchMonthList = new ArrayList<>();

		// Reading the model year and latest model year.
		DataFrame totalSeriesData = seriesFrameData
				.select(seriesFrameData.col(TrendingModelUtil
						.getValue("MODEL_YEAR")),
						seriesFrameData.col(TrendingModelUtil
								.getValue("BUSINESS_MONTH")),
						seriesFrameData.col(TrendingModelUtil
								.getValue("LAUNCH_MONTH"))).sort(
						seriesFrameData.col(
								TrendingModelUtil.getValue("BUSINESS_MONTH"))
								.desc());

		// Reading the all the launch month indicator business month and model
		// year.
		Map<String, Integer> launchMap = new HashMap<>();
		for (Row row : totalSeriesData.collect()) {
			int modelYear = row.isNullAt(0) ? 0 : row.getInt(0);
			String businessMonth = row.isNullAt(1) ? "" : row.getString(1);
			int launchMonthIndicator = row.isNullAt(2) ? 0 : row.getInt(2);
			if (launchMonthIndicator == 1) {
				launchMap.put(businessMonth, modelYear);
			}
		}
		logger.info("launchMap:" + launchMap);

		// Checking and putting the logic to remove unnecessary rows.
		// Reading the all the launch month indicator business month and model
		// year.
		int previousLatestModelYear = 0;
		for (Row row : totalSeriesData.collect()) {
			int modelYear = row.isNullAt(0) ? 0 : row.getInt(0);
			String businessMonth = row.isNullAt(1) ? "" : row.getString(1);
			int launchMonthIndicator = row.isNullAt(2) ? 0 : row.getInt(2);

			int latestModelYear = launchMap.containsKey(businessMonth) ? launchMap
					.get(businessMonth) : 0;
			if (latestModelYear > 0) {
				previousLatestModelYear = latestModelYear;
			}

			logger.info("modelYear:" + modelYear + " : businessMonth: "
					+ businessMonth + " : launchMonthIndicator: "
					+ launchMonthIndicator + " : latestModelYear: "
					+ latestModelYear + " : previousLatestModelYear: "
					+ previousLatestModelYear);
			if (launchMonthIndicator == 1) {
				if (previousLatestModelYear == modelYear) {
					launchMonthList.add(1);
				}
			} else {
				if (previousLatestModelYear == modelYear) {
					launchMonthList.add(-1);
				} else {
					launchMonthList.add(0);
				}
			}
		}

		// Putting the total series data by descending order using business
		// month.
		DataFrame sortedFrame = seriesFrameData.sort(seriesFrameData.col(
				TrendingModelUtil.getValue("BUSINESS_MONTH")).desc());
		seriesFrameData = addOutputColumnsToDataFrame(
				sortedFrame.collectAsList(), sortedFrame,
				addLauchColumnToSeriesFrame(launchMonthList).entrySet());
		logger.info("count before remove: " + seriesFrameData.count());

		// Filtering the rows with the positive values.
		seriesFrameData = seriesFrameData.filter(seriesFrameData.col(
				TrendingModelUtil.getValue("LAUNCH_MONTH_IND")).$greater$eq(0));
		seriesFrameData = seriesFrameData.sort(seriesFrameData.col(
				TrendingModelUtil.getValue("BUSINESS_MONTH")).asc());
		logger.info("count after remove: " + seriesFrameData.count());

		logger.info("launchMonthList: " + launchMonthList);
		logger.info("removeUncessaryRows end.");
		return seriesFrameData;
	}

	/**
	 * This method calculates the new model year flag and result write to future
	 * frame.
	 * 
	 * @param seriesFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public Map<String, List<Integer>> addLauchColumnToSeriesFrame(
			List<Integer> launchList) throws AnalyticsRuntimeException {
		logger.info("addLauchColumnToSeriesFrame start.");
		// Initializing the column values.
		Map<String, List<Integer>> forecastMap = new HashMap<>();

		// Adding the segment non fleet percent.
		forecastMap.put(TrendingModelUtil.getValue("LAUNCH_MONTH_IND"),
				launchList);

		logger.info("addLauchColumnToSeriesFrame end.");
		return forecastMap;
	}
}
