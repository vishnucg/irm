/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
 * This class contains methods which will do segment related calculations like
 * segment seasonal factors, segment share, retail segment share based on the
 * formulas.
 * 
 * @author Naresh
 *
 */
public class SegmentCalculations implements Serializable {

	private static final long serialVersionUID = 1922753362130818479L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(SegmentCalculations.class);

	/**
	 * This method calculates the segment volume based on the segment volume and
	 * seasonal factor for particular business month. Here, This segment annual
	 * volume divided by 12 and multiplied with respective month seasonal factor
	 * value.
	 * 
	 * @param futureDataFrame
	 * @param monthCoefficientFactors
	 * @return
	 */
	public List<Double> populateSegmentVolume(DataFrame futureDataFrame,
			Map<String, Double> monthCoefficientFactors) {

		// Select segment_volume_annual and business_month.
		DataFrame segSeasonFactorFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("SEGMENT_VOLUME_ANNUAL")),
				futureDataFrame.col(TrendingModelUtil
						.getValue("BUSINESS_MONTH")));

		// Loop through the frame and do the calculations.
		List<Double> segmentVolumeList = new ArrayList<>();
		for (Row row : segSeasonFactorFrame.collect()) {
			double segmentVolumeAnnual = row.isNullAt(0) ? 0.0 : row
					.getDouble(0);
			String businessMonth = row.getString(1).substring(4);
			double volume = (segmentVolumeAnnual / 12)
					* (monthCoefficientFactors.get(businessMonth));
			segmentVolumeList.add(volume);
		}
		return segmentVolumeList;
	}

	/**
	 * This method calculates the seg_share of average of seg_share last three
	 * months in the history data and multiplied with the seasonal factor of
	 * particular month.
	 * 
	 * @param futureDataFrame
	 * @param monthCoefficientFactors
	 * @param historyDataFrame
	 * @return
	 */
	public List<Double> calculateSegshare(DataFrame futureDataFrame,
			Map<String, Double> monthCoefficientFactors,
			DataFrame historyDataFrame) {

		// Select business_month.
		DataFrame segShareFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("BUSINESS_MONTH")));

		// Calculate the last 3 months average seg_share.
		double averageSegShare = TrendingModelUtil.averageNumberOfRowsInFrame(
				historyDataFrame, TrendingModelUtil.getValue("SEG_SHARE"), 3);

		// Loop through the frame and do the calculations.
		List<Double> segShareList = new ArrayList<>();
		for (Row row : segShareFrame.collect()) {
			String businessMonth = row.getString(0).substring(4);
			double segShare = averageSegShare
					* (monthCoefficientFactors.get(businessMonth));
			segShareList.add(segShare);
		}
		return segShareList;
	}

	/**
	 * This method calculates the retail_seg_share based on the average of 12
	 * months retail_seg_share from the history frame and multiplied with the
	 * respective month seasonal factor.
	 * 
	 * @param futureDataFrame
	 * @param monthCoefficientFactors
	 * @return
	 */
	public List<Double> calculateSegmentNonFleetpct(DataFrame futureDataFrame,
			Map<String, Double> monthCoefficientFactors, DataFrame historyFrame) {

		// Select business_month.
		DataFrame nonFleetPctFrame = futureDataFrame.select(futureDataFrame
				.col(TrendingModelUtil.getValue("BUSINESS_MONTH")));

		// Calculating the average non fleet percent for year.
		double averageNonFleetValue = TrendingModelUtil
				.averageNumberOfRowsInFrame(historyFrame,
						TrendingModelUtil.getValue("SEGMENT_NON_FLEET_PCT"), 12);

		// Loop through the frame and do the calculations.
		List<Double> nonFleetPctlist = new ArrayList<>();
		for (Row row : nonFleetPctFrame.collect()) {
			String businessMonth = row.getString(0).substring(4);
			double nonFleetPct = averageNonFleetValue
					* (monthCoefficientFactors.get(businessMonth));
			nonFleetPctlist.add(nonFleetPct);
		}
		return nonFleetPctlist;
	}

	/**
	 * This method calculates the segment seasonal factor details, seg_share and
	 * retail_seg_share details.
	 * 
	 * @param futureDataFrame
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	private Map<String, List<Double>> generateSegmentSeasonalFactor(
			DataFrame futureDataFrame, Map<String, List<Double>> segmentMap,
			DataFrame historyFrame) throws AnalyticsRuntimeException {

		// Select segment_volume and business_month.
		DataFrame segSeasonFactorFrame = historyFrame.select(historyFrame
				.col(TrendingModelUtil.getValue("SEGMENT_VOLUME")),
				historyFrame.col(TrendingModelUtil.getValue("BUSINESS_MONTH")),
				historyFrame.col(TrendingModelUtil.getValue("SEG_SHARE")),
				historyFrame.col(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT")), historyFrame
						.col(TrendingModelUtil
								.getValue("SEGMENT_VOLUME_ANNUAL")));
		int numberOfLines = (int) futureDataFrame.count();
		segSeasonFactorFrame.show();

		// Generate the values for the for each month.
		try {
			JavaRDD<Integer> monthValues = predictSegSeasonalFactorValues(segSeasonFactorFrame);
			logger.info("monthValues: " + monthValues.count());

			// Reading all the months data.
			DataFrame finalDataFrame = addMonthColumnToDataFrame(
					segSeasonFactorFrame,
					monthValues.collect(),
					prepareMonthRegressionInput(generateAllMonthsValues(monthValues)));

			// Checking the month columns data.
			List<String> independentList = checkMonthColumnsData(
					finalDataFrame, readMonthIndepList());

			// Call regression on month values.
			TrendModelManager modelManager = new TrendModelManager();
			LinearModelDTO linearModel = modelManager
					.callRegressionModelWithNoIntercept(finalDataFrame,
							TrendingModelUtil.getValue("SEGMENT_VOLUME"),
							independentList, true);
			segmentMap = predictSegmentForecastValues(linearModel,
					finalDataFrame, segmentMap,
					TrendingModelUtil.getValue("SEGMENT_VOLUME"), numberOfLines);
			segmentMap.put(
					TrendingModelUtil.getValue("SEGMENT_VOLUME")
							+ TrendingModelUtil.getValue("OUTPUT"),
					populateSegmentVolume(futureDataFrame,
							getSegmentVolumeMonthFactors(segmentMap)));

			// Call segment seasonal factor.
			LinearModelDTO linearModelSegShare = modelManager
					.callRegressionModelWithNoIntercept(finalDataFrame,
							TrendingModelUtil.getValue("SEG_SHARE"),
							independentList, true);
			segmentMap = predictSegmentForecastValues(linearModelSegShare,
					futureDataFrame, segmentMap,
					TrendingModelUtil.getValue("SEG_SHARE"), numberOfLines);
			segmentMap
					.put(TrendingModelUtil.getValue("SEG_SHARE")
							+ TrendingModelUtil.getValue("OUTPUT"),
							calculateSegshare(futureDataFrame,
									getSegShareMonthFactors(segmentMap),
									finalDataFrame));

			// Call segment non fleet percent values.
			LinearModelDTO linearModelNonFleet = modelManager
					.callRegressionModelWithNoIntercept(
							finalDataFrame,
							TrendingModelUtil.getValue("SEGMENT_NON_FLEET_PCT"),
							independentList, true);
			segmentMap = predictSegmentForecastValues(linearModelNonFleet,
					futureDataFrame, segmentMap,
					TrendingModelUtil.getValue("SEGMENT_NON_FLEET_PCT"),
					numberOfLines);
			segmentMap.put(
					TrendingModelUtil.getValue("SEGMENT_NON_FLEET_PCT")
							+ TrendingModelUtil.getValue("OUTPUT"),
					calculateSegmentNonFleetpct(futureDataFrame,
							getNonFleetPctMonthFactors(segmentMap),
							finalDataFrame));
		} catch (AnalyticsRuntimeException analyticsRuntimeException) {
			throw new AnalyticsRuntimeException(
					analyticsRuntimeException.getMessage());
		}
		return segmentMap;
	}

	/**
	 * This method calculates the dealer_margin based on the MSRP and
	 * dealer_margin_percent.
	 * 
	 * @param trendInputFrame
	 * @return
	 */
	private List<Double> generateDealerMargin(DataFrame trendInputFrame) {
		// Collect data from the data frame.
		List<Double> rows = new ArrayList<>();

		// Collecting the data and calculating the dealer margin value.
		for (Row row : trendInputFrame.select(
				trendInputFrame.col(TrendingModelUtil.getValue("MSRP")),
				trendInputFrame.col(TrendingModelUtil
						.getValue("DEALER_MARGIN_PCT"))).collect()) {
			double msrp = row.isNullAt(0) ? 0.0 : row.getDouble(0);
			double dealerMarginPct = row.isNullAt(1) ? 0.0 : row.getDouble(1);
			rows.add((double) Math.round((msrp * dealerMarginPct) / 50) * 50);
		}
		return rows;
	}

	/**
	 * This method calculates dealer_margin, segment_seasonal_factors,
	 * seg_share, retail_seg_share and fleet_pct values.
	 * 
	 * @param forecastOutputFrame
	 * @param forecastMap
	 * @return
	 * @throws AnalyticsRuntimeException
	 */
	public Map<String, List<Double>> addColumnValues(
			DataFrame forecastOutputFrame,
			Map<String, List<Double>> forecastMap, DataFrame regressionFrame)
			throws AnalyticsRuntimeException {
		// Adding dealer margin details.
		forecastMap.put(TrendingModelUtil.getValue("DEALER_MARGIN")
				+ TrendingModelUtil.getValue("OUTPUT"),
				generateDealerMargin(forecastOutputFrame));

		// Adding the segment seasonal factor.
		forecastMap = generateSegmentSeasonalFactor(forecastOutputFrame,
				forecastMap, regressionFrame);

		return forecastMap;
	}

	/**
	 * This method reads the business month and take the month values from
	 * history frame.
	 * 
	 * @param historyFrame
	 * @return
	 */
	private JavaRDD<Integer> predictSegSeasonalFactorValues(
			DataFrame historyFrame) {
		return historyFrame.toJavaRDD().map(new Function<Row, Integer>() {
			private static final long serialVersionUID = 2245714652058215812L;

			@Override
			public Integer call(Row row) throws Exception {
				String businessMonth = row.getString(1);
				int monthValue;
				if (null != businessMonth && businessMonth.length() == 6) {
					monthValue = Integer.parseInt(businessMonth.substring(4));
				} else {
					monthValue = 1;
				}
				return monthValue;
			}
		});
	}

	/**
	 * This method read month values, based on particular month this will gives
	 * you list of values.
	 * 
	 * @param months
	 * @return
	 */
	private JavaRDD<MonthDTO> generateAllMonthsValues(JavaRDD<Integer> months) {
		return months.map(new Function<Integer, MonthDTO>() {
			private static final long serialVersionUID = 5915097829446352443L;

			@Override
			public MonthDTO call(Integer key) throws Exception {
				if (null != key) {
					return getMonthColumnValues(key);
				} else {
					return getMonthColumnValues(1);
				}
			}
		});
	}

	/**
	 * This method reads the month column values and divide and set it to
	 * MonthDTO to prepare 12 columns in history frame like
	 * jan,feb,mar,apr,may,june,july,aug,sep,oct,nov,dec.
	 * 
	 * @param key
	 * @return
	 */
	private MonthDTO getMonthColumnValues(Integer key) {
		MonthDTO monthDTO = null;
		switch (key) {
		case 1:
			monthDTO = generateMonthColumnValues("1,0,0,0,0,0,0,0,0,0,0,0");
			break;
		case 2:
			monthDTO = generateMonthColumnValues("0,1,0,0,0,0,0,0,0,0,0,0");
			break;
		case 3:
			monthDTO = generateMonthColumnValues("0,0,1,0,0,0,0,0,0,0,0,0");
			break;
		case 4:
			monthDTO = generateMonthColumnValues("0,0,0,1,0,0,0,0,0,0,0,0");
			break;
		case 5:
			monthDTO = generateMonthColumnValues("0,0,0,0,1,0,0,0,0,0,0,0");
			break;
		case 6:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,1,0,0,0,0,0,0");
			break;
		case 7:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,0,1,0,0,0,0,0");
			break;
		case 8:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,0,0,1,0,0,0,0");
			break;
		case 9:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,0,0,0,1,0,0,0");
			break;
		case 10:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,0,0,0,0,1,0,0");
			break;
		case 11:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,0,0,0,0,0,1,0");
			break;
		case 12:
			monthDTO = generateMonthColumnValues("0,0,0,0,0,0,0,0,0,0,0,1");
			break;
		default:
			break;
		}
		return monthDTO;
	}

	/**
	 * This method reads the corresponding values and set to DTO as per month
	 * wise.
	 * 
	 * @param monthValues
	 * @return
	 */
	private MonthDTO generateMonthColumnValues(String monthValues) {
		MonthDTO monthDTO = new MonthDTO();
		String[] values = StringUtils.split(monthValues, ",");
		monthDTO.setJanValue(Integer.parseInt(values[0]));
		monthDTO.setFebValue(Integer.parseInt(values[1]));
		monthDTO.setMarValue(Integer.parseInt(values[2]));
		monthDTO.setAprValue(Integer.parseInt(values[3]));
		monthDTO.setMayValue(Integer.parseInt(values[4]));
		monthDTO.setJuneValue(Integer.parseInt(values[5]));
		monthDTO.setJulyValue(Integer.parseInt(values[6]));
		monthDTO.setAugValue(Integer.parseInt(values[7]));
		monthDTO.setSepValue(Integer.parseInt(values[8]));
		monthDTO.setOctValue(Integer.parseInt(values[9]));
		monthDTO.setNovValue(Integer.parseInt(values[10]));
		monthDTO.setDecValue(Integer.parseInt(values[11]));
		return monthDTO;
	}

	/**
	 * This method reads all the data from DTO's and prepare the list of values
	 * for each column.
	 * 
	 * @param monthvalueColumns
	 * @return
	 */
	private Map<String, List<Integer>> prepareMonthRegressionInput(
			JavaRDD<MonthDTO> monthvalueColumns) {

		// All the columns data.
		Map<String, List<Integer>> monthsMap = new LinkedHashMap<>();
		List<Integer> janList = new ArrayList<>();
		List<Integer> febList = new ArrayList<>();
		List<Integer> marList = new ArrayList<>();
		List<Integer> aprList = new ArrayList<>();
		List<Integer> mayList = new ArrayList<>();
		List<Integer> juneList = new ArrayList<>();
		List<Integer> julyList = new ArrayList<>();
		List<Integer> augList = new ArrayList<>();
		List<Integer> sepList = new ArrayList<>();
		List<Integer> octList = new ArrayList<>();
		List<Integer> novList = new ArrayList<>();
		List<Integer> decList = new ArrayList<>();

		// Collecting the data.
		for (MonthDTO monthDTO : monthvalueColumns.collect()) {
			janList.add(monthDTO.getJanValue());
			febList.add(monthDTO.getFebValue());
			marList.add(monthDTO.getMarValue());
			aprList.add(monthDTO.getAprValue());
			mayList.add(monthDTO.getMayValue());
			juneList.add(monthDTO.getJuneValue());
			julyList.add(monthDTO.getJulyValue());
			augList.add(monthDTO.getAugValue());
			sepList.add(monthDTO.getSepValue());
			octList.add(monthDTO.getOctValue());
			novList.add(monthDTO.getNovValue());
			decList.add(monthDTO.getDecValue());
		}

		// Adding to map.
		monthsMap.put("jan", janList);
		monthsMap.put("feb", febList);
		monthsMap.put("mar", marList);
		monthsMap.put("apr", aprList);
		monthsMap.put("may", mayList);
		monthsMap.put("june", juneList);
		monthsMap.put("july", julyList);
		monthsMap.put("aug", augList);
		monthsMap.put("sep", sepList);
		monthsMap.put("oct", octList);
		monthsMap.put("nov", novList);
		monthsMap.put("dec", decList);
		return monthsMap;
	}

	/**
	 * This method adds the MSRP Forecast values to input frame.
	 * 
	 * @param historyFrame
	 * @param monthValues
	 * @return
	 */
	private DataFrame addMonthColumnToDataFrame(DataFrame historyFrame,
			List<Integer> monthValues, Map<String, List<Integer>> mapMonths) {
		List<Row> rows = historyFrame.collectAsList();
		List<Row> rowsWithForecastValues = new ArrayList<>(rows.size());

		// Reading the forecast values.
		for (int j = 0; j < rows.size(); j++) {
			List<Object> newRow = new ArrayList<>(
					JavaConversions.asJavaList(rows.get(j).toSeq()));
			for (Map.Entry<String, List<Integer>> entry : mapMonths.entrySet()) {
				List<Integer> listValues = entry.getValue();
				newRow.add(listValues.get(j));
			}
			newRow.add(monthValues.get(j));
			rowsWithForecastValues.add(RowFactory.create(newRow
					.toArray(new Object[newRow.size()])));
		}

		// Adding values to input frame.
		JavaRDD<Row> rowJavaRDD = TrendingModelUtil.getJavaSparkContext()
				.parallelize(rowsWithForecastValues);
		List<StructField> structFields = new ArrayList<>(
				JavaConversions.asJavaList(historyFrame.schema()));
		for (Map.Entry<String, List<Integer>> entry : mapMonths.entrySet()) {
			String columnName = entry.getKey();
			structFields.add(new StructField(columnName, DataTypes.IntegerType,
					true, Metadata.empty()));
		}
		StructType structType = new StructType(
				structFields.toArray(new StructField[structFields.size()]));
		historyFrame = TrendingModelUtil.getHivecontext().createDataFrame(
				rowJavaRDD, structType);
		return historyFrame;
	}

	/**
	 * This method predicts segment seasonal factor values.
	 * 
	 * @param linearModelInformation
	 * @param futureDataFrame
	 * @param segmentMap
	 * @param dependentVariable
	 * @param numberOfLines
	 * @return
	 */
	private Map<String, List<Double>> predictSegmentForecastValues(
			LinearModelDTO linearModelInformation, DataFrame futureDataFrame,
			Map<String, List<Double>> segmentMap, String dependentVariable,
			int numberOfLines) {

		// Initializing the lists for the predict and forecast values.
		List<Double> janSesonalFactorValues = new ArrayList<>();
		List<Double> febSesonalFactorValues = new ArrayList<>();
		List<Double> marSesonalFactorValues = new ArrayList<>();
		List<Double> aprSesonalFactorValues = new ArrayList<>();
		List<Double> maySesonalFactorValues = new ArrayList<>();
		List<Double> juneSesonalFactorValues = new ArrayList<>();
		List<Double> julySesonalFactorValues = new ArrayList<>();
		List<Double> augSesonalFactorValues = new ArrayList<>();
		List<Double> sepSesonalFactorValues = new ArrayList<>();
		List<Double> octSesonalFactorValues = new ArrayList<>();
		List<Double> novSesonalFactorValues = new ArrayList<>();
		List<Double> decSesonalFactorValues = new ArrayList<>();

		// Reading the column coefficients list and intercept.
		Map<String, Double> values = linearModelInformation
				.getCoefficientValues();
		double janCoefficient;
		double febCoefficient;
		double marCoefficient;
		double aprCoefficient;
		double mayCoefficient;
		double juneCoefficient;
		double julyCoefficient;
		double augCoefficient;
		double sepCoefficient;
		double octCoefficient;
		double novCoefficient;
		double decCoefficient;

		if (null != values) {
			// Reading the major and minor change coefficients values.
			janCoefficient = values.containsKey("jan") ? values.get("jan")
					: 0.0;
			febCoefficient = values.containsKey("feb") ? values.get("feb")
					: 0.0;
			marCoefficient = values.containsKey("mar") ? values.get("mar")
					: 0.0;
			aprCoefficient = values.containsKey("apr") ? values.get("apr")
					: 0.0;
			mayCoefficient = values.containsKey("may") ? values.get("may")
					: 0.0;
			juneCoefficient = values.containsKey("june") ? values.get("june")
					: 0.0;
			julyCoefficient = values.containsKey("july") ? values.get("july")
					: 0.0;
			augCoefficient = values.containsKey("aug") ? values.get("aug")
					: 0.0;
			sepCoefficient = values.containsKey("sep") ? values.get("sep")
					: 0.0;
			octCoefficient = values.containsKey("oct") ? values.get("oct")
					: 0.0;
			novCoefficient = values.containsKey("nov") ? values.get("nov")
					: 0.0;
			decCoefficient = values.containsKey("dec") ? values.get("dec")
					: 0.0;
		} else {
			janCoefficient = 0.0;
			febCoefficient = 0.0;
			marCoefficient = 0.0;
			aprCoefficient = 0.0;
			mayCoefficient = 0.0;
			juneCoefficient = 0.0;
			julyCoefficient = 0.0;
			augCoefficient = 0.0;
			sepCoefficient = 0.0;
			octCoefficient = 0.0;
			novCoefficient = 0.0;
			decCoefficient = 0.0;
		}

		// Taking the sum of all seasonal factor.
		double sumeOfFactors = janCoefficient + febCoefficient + marCoefficient
				+ aprCoefficient + mayCoefficient + juneCoefficient
				+ julyCoefficient + augCoefficient + sepCoefficient
				+ octCoefficient + novCoefficient + decCoefficient;

		// Calculating the seasonal factor.
		double janSeasonalFactor = (janCoefficient / sumeOfFactors) * 12;
		double febSeasonalFactor = (febCoefficient / sumeOfFactors) * 12;
		double marSeasonalFactor = (marCoefficient / sumeOfFactors) * 12;
		double aprSeasonalFactor = (aprCoefficient / sumeOfFactors) * 12;
		double maySeasonalFactor = (mayCoefficient / sumeOfFactors) * 12;
		double juneSeasonalFactor = (juneCoefficient / sumeOfFactors) * 12;
		double julySeasonalFactor = (julyCoefficient / sumeOfFactors) * 12;
		double augSeasonalFactor = (augCoefficient / sumeOfFactors) * 12;
		double sepSeasonalFactor = (sepCoefficient / sumeOfFactors) * 12;
		double octSeasonalFactor = (octCoefficient / sumeOfFactors) * 12;
		double novSeasonalFactor = (novCoefficient / sumeOfFactors) * 12;
		double decSeasonalFactor = (decCoefficient / sumeOfFactors) * 12;

		// Collecting all the rows and do calculations.
		for (int counter = 0; counter < numberOfLines; counter++) {
			janSesonalFactorValues.add(janSeasonalFactor);
			febSesonalFactorValues.add(febSeasonalFactor);
			marSesonalFactorValues.add(marSeasonalFactor);
			aprSesonalFactorValues.add(aprSeasonalFactor);
			maySesonalFactorValues.add(maySeasonalFactor);
			juneSesonalFactorValues.add(juneSeasonalFactor);
			julySesonalFactorValues.add(julySeasonalFactor);
			augSesonalFactorValues.add(augSeasonalFactor);
			sepSesonalFactorValues.add(sepSeasonalFactor);
			octSesonalFactorValues.add(octSeasonalFactor);
			novSesonalFactorValues.add(novSeasonalFactor);
			decSesonalFactorValues.add(decSeasonalFactor);
		}

		// Setting up the seasonal factors.
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("JAN_SESONAL_FACTOR"),
				janSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("FEB_SESONAL_FACTOR"),
				febSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("MAR_SESONAL_FACTOR"),
				marSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("APR_SESONAL_FACTOR"),
				aprSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("MAY_SESONAL_FACTOR"),
				maySesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("JUNE_SESONAL_FACTOR"),
				juneSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("JULY_SESONAL_FACTOR"),
				julySesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("AUG_SESONAL_FACTOR"),
				augSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("SEP_SESONAL_FACTOR"),
				sepSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("OCT_SESONAL_FACTOR"),
				octSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("NOV_SESONAL_FACTOR"),
				novSesonalFactorValues);
		segmentMap.put(
				dependentVariable
						+ TrendingModelUtil.getValue("DEC_SESONAL_FACTOR"),
				decSesonalFactorValues);
		return segmentMap;
	}

	/**
	 * This method reads the months independent list and will give you list of
	 * variables.
	 * 
	 * @return
	 */
	private List<String> readMonthIndepList() {
		String monthsList = TrendingModelUtil.getValue("MONTH_INDEP_LIST");
		String[] monthsValuesList = StringUtils.split(monthsList, ",");
		List<String> monthsVariables = new ArrayList<>();
		for (String variable : monthsValuesList) {
			monthsVariables.add(variable.toLowerCase().trim());
		}
		return monthsVariables;
	}

	/**
	 * This method reads the segment volume month factors as a Map, here key is
	 * factor name and value is particular month segment seasonal factor.
	 * 
	 * @param forecastMap
	 * @return
	 */
	public Map<String, Double> getSegmentVolumeMonthFactors(
			Map<String, List<Double>> forecastMap) {
		Map<String, Double> monthFactorValues = new LinkedHashMap<>();
		if (null != forecastMap && !forecastMap.isEmpty()) {

			// Putting the segment volume month factors in map.
			for (Entry<String, List<Double>> value : forecastMap.entrySet()) {
				String key = value.getKey();
				List<Double> factorValue = value.getValue();
				if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_JAN_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_ONE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_FEB_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TWO"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_MAR_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_THREE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_APR_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_FOUR"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_MAY_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_FIVE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_JUNE_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_SIX"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_JULY_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_SEVEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_AUG_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_EIGHT"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_SEP_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_NINE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_OCT_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_NOV_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_ELEVEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_VOLUME_DEC_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TWELEVE"),
							factorValue.get(0));
				}
			}
		}
		return monthFactorValues;
	}

	/**
	 * * This method reads the seg_share month factors as a Map, here key is
	 * seg_share name and value is particular month segment seasonal factor.
	 * 
	 * @param forecastMap
	 * @return
	 */
	public Map<String, Double> getSegShareMonthFactors(
			Map<String, List<Double>> forecastMap) {
		Map<String, Double> monthFactorValues = new LinkedHashMap<>();
		if (null != forecastMap && !forecastMap.isEmpty()) {

			// Putting the seg share month factors in the Map.
			for (Entry<String, List<Double>> value : forecastMap.entrySet()) {
				String key = value.getKey();
				List<Double> factorValue = value.getValue();
				if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_JAN_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_ONE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_FEB_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TWO"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_MAR_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_THREE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_APR_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_FOUR"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_MAY_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_FIVE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_JUNE_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_SIX"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_JULY_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_SEVEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_AUG_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_EIGHT"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_SEP_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_NINE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_OCT_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_NOV_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_ELEVEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEG_SHARE_DEC_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TWELEVE"),
							factorValue.get(0));
				}
			}
		}
		return monthFactorValues;
	}

	/**
	 * * This method reads the retail_seg_share month factors as a Map, here key
	 * is retail_share_month name and value is particular month segment seasonal
	 * factor.
	 * 
	 * @param forecastMap
	 * @return
	 */
	public Map<String, Double> getNonFleetPctMonthFactors(
			Map<String, List<Double>> forecastMap) {
		Map<String, Double> monthFactorValues = new LinkedHashMap<>();
		if (null != forecastMap && !forecastMap.isEmpty()) {

			// Putting the non fleet percent month factors
			for (Entry<String, List<Double>> value : forecastMap.entrySet()) {
				String key = value.getKey();
				List<Double> factorValue = value.getValue();
				if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_JAN_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_ONE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_FEB_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TWO"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_MAR_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_THREE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_APR_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_FOUR"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_MAY_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_FIVE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_JUNE_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_SIX"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_JULY_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_SEVEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_AUG_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_EIGHT"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_SEP_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_NINE"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_OCT_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_NOV_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_ELEVEN"),
							factorValue.get(0));
				} else if (key.equals(TrendingModelUtil
						.getValue("SEGMENT_NON_FLEET_PCT_DEC_FACTOR"))) {
					monthFactorValues.put(
							TrendingModelUtil.getValue("MONTH_TWELEVE"),
							factorValue.get(0));
				}
			}
		}
		return monthFactorValues;
	}

	/**
	 * This method reads the final data frame and checks each and every column
	 * in the independent variables list whether that columns has same values or
	 * not. If the column has same values, then it removes from the list finally
	 * it returns the independent variables list.
	 * 
	 * @param finalFrame
	 * @param independentVariables
	 * @return
	 */
	private List<String> checkMonthColumnsData(DataFrame finalFrame,
			List<String> independentVariables) {
		// Collecting the final list which are having the same column values in
		// the list.
		List<String> finalList = new ArrayList<>();
		if (null != independentVariables && !independentVariables.isEmpty()) {
			for (String variable : independentVariables) {
				DataFrame columnData = finalFrame.select(
						finalFrame.col(variable)).distinct();
				if (columnData.count() < 2) {
					finalList.add(variable);
				}
			}

			// Removing the columns which are having the same values.
			for (String var : finalList) {
				independentVariables.remove(var);
			}
		}
		return independentVariables;
	}
}
