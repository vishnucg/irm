package regression;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.hive.HiveContext;

import util.MRMIndexes;
import util.NetworkCommunicator;
import bayesian.BasianFinalElasticity;
import bestDataset.PickbestDataset;
import dataPrep.DataPreparation;
import exceptions.MRMBaseException;
import exceptions.MRMDbPropertiesNotFoundException;
import exceptions.MRMHiveContextNotFoundException;
import exceptions.MRMInputTableDataNotFoundException;
import exceptions.MRMNoQueryParametersFoundException;
import exceptions.MRMNoSeriesRegionFoundException;
import exceptions.MRMPropertiesNotFoundException;

/**
 * @author Naresh
 *
 */
public class IncentiveElasticityModel implements Serializable {

	private static final long serialVersionUID = -8429856972269891292L;

	static final Logger mrmDevLogger = Logger
			.getLogger(IncentiveElasticityModel.class);

	/**
	 * This method returns the incentive elasticity model.
	 * 
	 * @param propertiesFilePath
	 * @throws IOException
	 * @throws MRMBaseException
	 */
	public void getIncentiveElasticityModel() throws IOException,
			MRMBaseException {

		// Reading the properties data.
		IncentiveElasticityModelDTO incentiveElasticityModelDTO = readPropertiesData();

		// Read the query parameters
		incentiveElasticityModelDTO.setQueryproperties(readQueryParameters());

		// Get the Hive Connection
		incentiveElasticityModelDTO
				.setHiveContext(getHiveConnection(incentiveElasticityModelDTO));

		// Read sales data.
		incentiveElasticityModelDTO = getSalesData(incentiveElasticityModelDTO);

		// ReadParameters table data.
		incentiveElasticityModelDTO = readParametersData(incentiveElasticityModelDTO);

		// Get Series and regions data.
		incentiveElasticityModelDTO = getSeriesRegionList(incentiveElasticityModelDTO);

		// Filter series and region data.
		incentiveElasticityModelDTO = filterSeriesData(incentiveElasticityModelDTO);

		// Reading shareInctvExclProperties data and inctvRatioExclProperties
		// values data.
		incentiveElasticityModelDTO = readPropertiesData(incentiveElasticityModelDTO);

		// Process region data
		processSeriesRegionsData(incentiveElasticityModelDTO);
	}

	/**
	 * This method reads the properties data like model, database name and user
	 * id.
	 * 
	 * @param propertiesFilePath
	 * @return incentiveElasticityModelDTO
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws MRMDbPropertiesNotFoundException
	 */
	private IncentiveElasticityModelDTO readPropertiesData()
			throws IOException, MRMDbPropertiesNotFoundException {
		// Loading the db properties.
		IncentiveElasticityModelDTO incentiveElasticityModelDTO = new IncentiveElasticityModelDTO();
		Properties properties = new Properties();
		properties.load(IncentiveElasticityModel.class
				.getResourceAsStream("/config.properties"));

		// Setting up the properties.
		if (properties.containsKey("model")) {
			incentiveElasticityModelDTO.setModel(properties
					.getProperty("model"));
		}
		if (properties.containsKey("database.name")) {
			incentiveElasticityModelDTO.setDatabaseName(properties
					.getProperty("database.name"));
		}
		if (properties.containsKey("user.id")) {
			incentiveElasticityModelDTO.setUserID(properties
					.getProperty("user.id"));
		}
		if (properties.size() < 3) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_DB_PROPS"));
			throw new MRMDbPropertiesNotFoundException(
					MRMIndexes.getValue("NO_DB_PROPS"));
		}
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method returns the hive connection.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return hiveContext
	 * @throws MRMHiveContextNotFoundException
	 * @throws MRMDbPropertiesNotFoundException
	 */
	private HiveContext getHiveConnection(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws MRMHiveContextNotFoundException,
			MRMDbPropertiesNotFoundException {
		// Get hive connection.
		HiveContext hiveContext = NetworkCommunicator.getHiveContext();
		if (null == hiveContext || null == incentiveElasticityModelDTO) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_HIVE_CON"));
			throw new MRMHiveContextNotFoundException(
					MRMIndexes.getValue("NO_HIVE_CON"));
		} else {
			if (null == incentiveElasticityModelDTO.getDatabaseName()) {
				mrmDevLogger.error(MRMIndexes.getValue("NO_DB_PROPS"));
				throw new MRMDbPropertiesNotFoundException(
						MRMIndexes.getValue("NO_DB_PROPS"));
			} else {
				// Connect to database.
				hiveContext.sql("use "
						+ incentiveElasticityModelDTO.getDatabaseName() + "");
			}
		}
		return hiveContext;
	}

	/**
	 * This method loads the query parameters.
	 * 
	 * @return query properties
	 * @throws IOException
	 * @throws MRMNoQueryParametersFoundException
	 */
	private Properties readQueryParameters() throws IOException,
			MRMNoQueryParametersFoundException {
		Properties queryproperties = new Properties();
		queryproperties.load(IncentiveElasticityModel.class
				.getResourceAsStream("/Query.properties"));
		if (queryproperties.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_QUERY_PARAMS"));
			throw new MRMNoQueryParametersFoundException(
					MRMIndexes.getValue("EMPTY_QUERY_PARAMS"));
		}
		return queryproperties;
	}

	/**
	 * This method reads the sales region data.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 * @throws MRMInputTableDataNotFoundException
	 */
	private IncentiveElasticityModelDTO getSalesData(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws MRMInputTableDataNotFoundException {
		// Initialize the map.
		Map<Integer, Integer> salesMap = new HashMap<>();

		// Calling the hive context.
		HiveContext hiveContext = incentiveElasticityModelDTO.getHiveContext();

		// Get the input table and set the input table.
		DataFrame inputTable = hiveContext.sql(incentiveElasticityModelDTO
				.getQueryproperties().getProperty("inputquery"));

		inputTable = changeDatatypes(inputTable);
		if (null == inputTable) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_INPUT_TABLE_DATA"));
			throw new MRMInputTableDataNotFoundException(
					MRMIndexes.getValue("NO_INPUT_TABLE_DATA"));
		} else {
			incentiveElasticityModelDTO.setInputTable(inputTable);
		}

		// Get the sales data and set to sales Map.
		DataFrame sales = hiveContext
				.sql("select region_area_cd,count(*) as volume from base_tms_us_vehicle_sales group by region_area_cd");
		for (Row sales_value : sales.collect()) {
			salesMap.put(Integer.parseInt(sales_value.getString(0)),
					(int) sales_value.getLong(1));
		}
		incentiveElasticityModelDTO.setSalesMap(salesMap);
		return incentiveElasticityModelDTO;
	}

	/***
	 * Changing the string data types into double values
	 * 
	 * @param inputTable
	 * @return
	 */
	private DataFrame changeDatatypes(DataFrame inputTable) {
		// Changing in_market_stage
		inputTable = inputTable.withColumnRenamed("in_market_stage",
				"in_market_stage_value");
		List<Row> rowarray = inputTable.select("in_market_stage_value")
				.distinct().sort("in_market_stage_value").collectAsList();

		List<Row> marketStagesRows = new ArrayList<Row>();
		for (Row distinct : rowarray) {
			if (distinct.getString(0) == null) {
				marketStagesRows.add(new GenericRow(new Object[] {
						distinct.getString(0),
						(double) rowarray.indexOf(distinct) }));
				continue;
			}
			marketStagesRows.add(new GenericRow(new Object[] {
					distinct.getString(0),
					(double) rowarray.indexOf(distinct) + 1 }));
		}
		String Query = "";
		// then 1.0 WHEN in_market_stage_value = 'LATE' then 2.0 WHEN
		// in_market_stage_value = 'MID1' then 3.0 WHEN in_market_stage_value =
		// 'MID2' then 4.0 else 0.0 end
		for (Row market : marketStagesRows) {
			Query += " WHEN in_market_stage_value = '" + market.getString(0)
					+ "' then " + market.getDouble(1);
		}
		inputTable = inputTable.selectExpr(
				"CASE " + Query + "  else 0.0 end" + " as in_market_stage",
				"make_nm",
				"series_nm",
				"car_truck",
				"model_year",
				"region_code",
				"month",
				"business_month", // "in_market_stage",
				"daily_sales", "apr_daily_sales", "apr_sales_share",
				"apr_incentive_comp_ratio", "apr_msrp_comp_ratio",
				"apr_comp_incentive", "lease_daily_sales", "lease_sales_share",
				"lease_incentive_comp_ratio", "lease_msrp_comp_ratio",
				"lease_comp_incentive", "cash_daily_sales", "cash_sales_share",
				"cash_incentive_comp_ratio", "cash_msrp_comp_ratio",
				"cash_comp_incentive", "cash_all_daily_sales",
				"cash_all_sales_share", "cash_all_incentive_comp_ratio",
				"cash_all_msrp_comp_ratio", "cash_all_comp_incentive",
				"contest_month_flg", "thrust_month_flg", "cftp",
				"life_cycle_position", "major_change", "minor_change",
				"comp_life_cycle_position", "comp_major_change",
				"comp_major_change_plus_1", "japan_tsunami", "gas_price",
				"gas_price_chg_pct");

		// Changing life_cycle_position
		inputTable = inputTable.withColumnRenamed("life_cycle_position",
				"life_cycle_position_value");
		List<Row> lifecyclearray = inputTable
				.select("life_cycle_position_value").distinct()
				.sort("life_cycle_position_value").collectAsList();

		List<Row> life_cycle_Rows = new ArrayList<Row>();
		for (Row distinct : lifecyclearray) {
			if (distinct.getString(0) == null) {
				life_cycle_Rows.add(new GenericRow(new Object[] {
						distinct.getString(0),
						(double) lifecyclearray.indexOf(distinct) }));
				continue;
			}
			life_cycle_Rows.add(new GenericRow(new Object[] {
					distinct.getString(0),
					(double) lifecyclearray.indexOf(distinct) + 1 }));
		}

		String Query1 = "";
		// then 1.0 WHEN in_market_stage_value = 'LATE' then 2.0 WHEN
		// in_market_stage_value = 'MID1' then 3.0 WHEN in_market_stage_value =
		// 'MID2' then 4.0 else 0.0 end
		for (Row life : life_cycle_Rows) {
			Query1 += " WHEN life_cycle_position_value = '" + life.getString(0)
					+ "' then " + life.getDouble(1);
		}
		inputTable = inputTable.selectExpr("CASE " + Query1 + "  else 0.0 end"
				+ " as life_cycle_position", "make_nm", "series_nm",
				"car_truck", "model_year", "region_code", "month",
				"business_month", "in_market_stage", "daily_sales",
				"apr_daily_sales", "apr_sales_share",
				"apr_incentive_comp_ratio", "apr_msrp_comp_ratio",
				"apr_comp_incentive", "lease_daily_sales", "lease_sales_share",
				"lease_incentive_comp_ratio", "lease_msrp_comp_ratio",
				"lease_comp_incentive", "cash_daily_sales", "cash_sales_share",
				"cash_incentive_comp_ratio", "cash_msrp_comp_ratio",
				"cash_comp_incentive", "cash_all_daily_sales",
				"cash_all_sales_share", "cash_all_incentive_comp_ratio",
				"cash_all_msrp_comp_ratio", "cash_all_comp_incentive",
				"contest_month_flg", "thrust_month_flg", "cftp",
				"major_change", "minor_change", "comp_life_cycle_position",
				"comp_major_change", "comp_major_change_plus_1",
				"japan_tsunami", "gas_price", "gas_price_chg_pct");

		// Changing COMP_LIFE_CYCLE_POSITION

		inputTable = inputTable.withColumnRenamed("comp_life_cycle_position",
				"comp_life_cycle_position_value");

		List<Row> complifecyclearray = inputTable
				.select("comp_life_cycle_position_value").distinct()
				.sort("comp_life_cycle_position_value").collectAsList();

		List<Row> comp_life_cycle_Rows = new ArrayList<Row>();
		for (Row distinct : complifecyclearray) {
			if (distinct.getString(0) == null) {
				continue;
			}
			comp_life_cycle_Rows.add(new GenericRow(new Object[] {
					distinct.getString(0),
					(double) complifecyclearray.indexOf(distinct) }));
		}
		String Query2 = "";
		for (Row comp : comp_life_cycle_Rows) {
			Query2 += " WHEN comp_life_cycle_position_value = '"
					+ comp.getString(0) + "' then " + comp.getDouble(1);
		}
		return inputTable = inputTable.selectExpr("CASE " + Query2
				+ "  else 0.0 end" + " as comp_life_cycle_position", "make_nm",
				"series_nm", "car_truck", "model_year", "region_code", "month",
				"business_month", "in_market_stage", "daily_sales",
				"life_cycle_position", "apr_daily_sales", "apr_sales_share",
				"apr_incentive_comp_ratio", "apr_msrp_comp_ratio",
				"apr_comp_incentive", "lease_daily_sales", "lease_sales_share",
				"lease_incentive_comp_ratio", "lease_msrp_comp_ratio",
				"lease_comp_incentive", "cash_daily_sales", "cash_sales_share",
				"cash_incentive_comp_ratio", "cash_msrp_comp_ratio",
				"cash_comp_incentive", "cash_all_daily_sales",
				"cash_all_sales_share", "cash_all_incentive_comp_ratio",
				"cash_all_msrp_comp_ratio", "cash_all_comp_incentive",
				"contest_month_flg", "thrust_month_flg", "cftp",
				"major_change", "minor_change", "comp_major_change",
				"comp_major_change_plus_1", "japan_tsunami", "gas_price",
				"gas_price_chg_pct");

	}

	/**
	 * This method reads the parameters data from the table.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 * @throws MRMPropertiesNotFoundException
	 */
	private IncentiveElasticityModelDTO readParametersData(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws MRMPropertiesNotFoundException {
		// Initialize the parameter map.
		Map<String, String> parameterMap = new HashMap<>();

		// Reading the parameter table data.
		DataFrame parameterTable = incentiveElasticityModelDTO
				.getHiveContext()
				.sql(incentiveElasticityModelDTO.getQueryproperties()
						.getProperty("parameter_table"))
				.filter("consumer_system_nm = '"
						+ incentiveElasticityModelDTO.getModel() + "'");
		incentiveElasticityModelDTO.setParameterTable(parameterTable);

		// Checking the parameter table properties.
		if (null != parameterTable) {
			for (Row parameter : parameterTable.collect()) {
				if (parameter.getString(1).toLowerCase().trim().substring(0, 1)
						.equals("c")) {
				} else {
					parameterMap.put(parameter.getString(0).toLowerCase()
							.trim(), parameter.getString(1).toLowerCase()
							.trim());
				}
			}
		}

		// Checking properties from the parameter table.
		if (parameterMap.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_PROPERTIES"));
			throw new MRMPropertiesNotFoundException(
					MRMIndexes.getValue("NO_PROPERTIES"));
		} else {
			parameterMap.put("prior.inctv.std.dev", "0.75");
			parameterMap.put("prior.adj.factor.no.best.model", "2");
			parameterMap.put("dep.var", "dependent_variable");
			parameterMap.put("incl.var", "incentive_comp_ratio");
			incentiveElasticityModelDTO.setParameterMap(parameterMap);
		}
		return incentiveElasticityModelDTO;
	}

	/**
	 * This sets the series and region values and send back.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 * @throws MRMNoSeriesRegionFoundException
	 */
	private IncentiveElasticityModelDTO getSeriesRegionList(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws MRMNoSeriesRegionFoundException {
		// Initialize the series and region lists.
		List<String> seriesList = new ArrayList<>();
		List<Integer> regionList = new ArrayList<>();
		List<String> regionValues = new ArrayList<>();

		// Get hive context and query properties.
		HiveContext hiveContext = incentiveElasticityModelDTO.getHiveContext();
		Properties queryProperties = incentiveElasticityModelDTO
				.getQueryproperties();

		// Get the time stamp.
		Timestamp timestamp = (Timestamp) hiveContext
				.sql(queryProperties.getProperty("timestampquery"))
				.filter("create_user_id = '"
						+ incentiveElasticityModelDTO.getUserID()
						+ "' and consumer_system_nm = '"
						+ incentiveElasticityModelDTO.getModel() + "'")
				.collect()[0].get(0);
		incentiveElasticityModelDTO.setTimeStamp(timestamp);

		// Get the series data frame.
		DataFrame seriesDataframe = hiveContext.sql(
				queryProperties.getProperty("series_dataframe")).filter(
				"create_ts = CAST('" + timestamp
						+ "' as TIMESTAMP) and create_user_id  = '"
						+ incentiveElasticityModelDTO.getUserID()
						+ "' and consumer_system_nm = '"
						+ incentiveElasticityModelDTO.getModel() + "'");

		if (null != seriesDataframe) {
			for (Row SeriesRegion : seriesDataframe.collect()) {
				if (SeriesRegion.get(0).equals("MAKE_NM,SERIES_NM,GRADE_NM"))
					seriesList
							.add(SeriesRegion.get(1).toString().split(",")[1]);

				if (SeriesRegion.get(0).equals("REGION_CODE"))
					regionValues.add(SeriesRegion.getString(1));
			}
		}

		// Checking the series and regions data.
		if (seriesList.isEmpty() || regionValues.isEmpty()) {
			throw new MRMNoSeriesRegionFoundException(
					MRMIndexes.getValue("NO_SERIES_REGION"));
		} else {
			// Setting the series and region lists.
			incentiveElasticityModelDTO.setSeriesList(seriesList);
			incentiveElasticityModelDTO.setRegionList(regionList);
			incentiveElasticityModelDTO.setRegionValues(regionValues);
			mrmDevLogger.warn("seriesList: " + seriesList + " regionList: "
					+ regionList + " regionValues:" + regionValues);
		}
		return incentiveElasticityModelDTO;
	}

	/**
	 * 
	 * @param userid
	 * @param series
	 * @param regionCode
	 * @param incentive
	 * @param elsticity
	 * @param independentVariables
	 */
	public static void saveoutput(String userid, String series,
			String regionCode, String incentive, Double elsticity,
			String independentVariables) {
		NetworkCommunicator.hiveContext
				.sql("CREATE TABLE IF NOT EXISTS mrm_execution_number (Rowid INT)");

		// Creating a output table to store the final elasticity
		NetworkCommunicator.hiveContext
				.sql("CREATE TABLE IF NOT EXISTS mrm_output_table (Rowid INT ,UserID INT, Series STRING, Region STRING, Incentive_type STRING, Elasticity DOUBLE,ElasticityDriver String, Execution_date STRING)");

		// Executuion number is the rowid which increments for every spark
		// submit
		DataFrame executionNumberDf = NetworkCommunicator.getHiveContext().sql(
				"select count(*) from mrm_execution_number");

		Long executionNumber = executionNumberDf.collect()[0].getLong(0) + 1;

		DataFrame executionumberrecord = NetworkCommunicator.hiveContext
				.sql("select " + executionNumber + " as Rowid ");
		executionumberrecord.saveAsTable("mrm_execution_number",
				SaveMode.Append);

		// Adding Current date in simple date format
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");

		// Creating a output dataframe with respective column names
		DataFrame outRecord = NetworkCommunicator.hiveContext.sql("select "
				+ executionNumber + " as Rowid," + userid + " as userid, "
				+ series + " as Series, " + regionCode + " as Region, '"
				+ incentive + "' as Incentive_type, " + elsticity
				+ " as Elasticity, '" + independentVariables
				+ "' as ElasticityDriver, '"
				+ simpleDateFormat.format(new Date()) + "' as Execution_date");

		// save that data frame into a hive table
		outRecord.saveAsTable("mrm_output_table", SaveMode.Append);

	}

	/**
	 * This method filters the series and region data.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 */
	private IncentiveElasticityModelDTO filterSeriesData(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO) {
		List<String> seriesList = incentiveElasticityModelDTO.getSeriesList();
		List<String> regionValues = incentiveElasticityModelDTO
				.getRegionValues();
		List<Integer> regionList = incentiveElasticityModelDTO.getRegionList();

		// Get hive context and query properties.
		HiveContext hiveContext = incentiveElasticityModelDTO.getHiveContext();
		Properties queryProperties = incentiveElasticityModelDTO
				.getQueryproperties();

		// Checking the series data.
		if (seriesList.isEmpty() || seriesList.contains("ALL")) {
			seriesList.clear();
			DataFrame seriesTable = hiveContext.sql(queryProperties
					.getProperty("series_table"));
			incentiveElasticityModelDTO.setSeriesTable(seriesTable);
			if (null != seriesTable) {
				for (Row series_value : seriesTable.collect()) {
					seriesList.add(series_value.get(0).toString());
				}
			}
		}
		mrmDevLogger.warn("Selected series list ::: " + seriesList);

		// Check the region data.
		if (regionValues.isEmpty() || regionValues.contains("ALL")
				|| regionValues.contains("NATIONAL")) {
			DataFrame regionTable = hiveContext.sql(queryProperties
					.getProperty("region_table"));
			incentiveElasticityModelDTO.setRegionTable(regionTable);
			if (null != regionTable) {
				for (Row filter_value : regionTable.collect()) {
					incentiveElasticityModelDTO.getRegionList().add(
							filter_value.getInt(0));
				}
			}
		} else {
			for (String filter_value : incentiveElasticityModelDTO
					.getRegionValues()) {
				regionList.add(Integer.parseInt(filter_value));
			}
		}
		mrmDevLogger.warn("Selected region list ::: " + regionList);

		incentiveElasticityModelDTO.setSeriesList(seriesList);
		incentiveElasticityModelDTO.setRegionList(regionList);
		incentiveElasticityModelDTO.setRegionValues(regionValues);
		mrmDevLogger.warn("seriesList: " + seriesList + " regionList: "
				+ regionList + " regionValues:" + regionValues);
		return incentiveElasticityModelDTO;
	}

	/**
	 * 
	 * @param incentiveElasticityModelDTO
	 * @throws IOException
	 * @throws MRMBaseException
	 */
	private void processSeriesRegionsData(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws IOException, MRMBaseException {
		// Take the series and region data.
		List<String> seriesList = incentiveElasticityModelDTO.getSeriesList();
		List<Integer> regionList = incentiveElasticityModelDTO.getRegionList();

		// Checking the series and regions data.
		if (seriesList.isEmpty() || regionList.isEmpty()) {
			throw new MRMNoSeriesRegionFoundException(
					MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			Map<String, String> parameterMap = incentiveElasticityModelDTO
					.getParameterMap();

			// Looping each row in the series array
			for (String series : seriesList) {
				// looping through list of regions and picking a particular
				// region
				incentiveElasticityModelDTO.setSeries(series);
				for (Integer regionCode : regionList) {
					// String[] incentive_array = { "apr" };

					if (parameterMap.containsKey("inctv_type_list")) {
						String[] incentiveValues = parameterMap
								.get("inctv_type_list").toLowerCase()
								.split(",");
						incentiveElasticityModelDTO.setRegion(regionCode);

						// Process Series, region and one incentive Data.
						processIncentivesData(incentiveValues,
								incentiveElasticityModelDTO);
					}
				}
			}
		}
	}

	/**
	 * 
	 * @param incentiveValues
	 * @param incentiveElasticityModelDTO
	 * @param series
	 * @param regionCode
	 * @param independentVariable
	 * @throws IOException
	 * @throws MRMBaseException
	 */
	private void processIncentivesData(String[] incentiveValues,
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws IOException, MRMBaseException {
		Double sumelasticity = 0.0;
		int elasticitycount = 0;

		// Taking the input table data.
		DataFrame inputTable = incentiveElasticityModelDTO.getInputTable();
		PickbestDataset pickbest = new PickbestDataset();
		DataPreparation datapreparation = new DataPreparation();

		// Looping all incentives.
		for (String incentive : incentiveValues) {
			incentive = incentive.trim();
			incentiveElasticityModelDTO.setIncentive(incentive);

			// read input data based on the below filters.
			DataFrame inputData = inputTable.filter(incentive
					+ "_daily_sales >0.0 and " + incentive
					+ "_incentive_comp_ratio >0.0  and series_nm = '"
					+ incentiveElasticityModelDTO.getSeries()
					+ "' and region_code ="
					+ incentiveElasticityModelDTO.getRegion() + "");
			mrmDevLogger.warn("Input data count after series filter :::"
					+ inputData.count());
			if (null != inputData) {
				if (inputData.count() < 5) {
					mrmDevLogger
							.info(" ************** Not enough input data to go furthur *********** series: "
									+ incentiveElasticityModelDTO.getSeries()
									+ "***** region: "
									+ incentiveElasticityModelDTO.getRegion()
									+ "****** incentive: " + incentive);
					continue;
				}
			}

			// Preparing the data.
			DataFrame dataOutlier = datapreparation.prepareData(inputData,
					incentive);

			if (null != dataOutlier) {
				if (dataOutlier.count() <= 0) {
					mrmDevLogger
							.info("There is no enough data to do the calculation  series: "
									+ incentiveElasticityModelDTO.getSeries()
									+ "***** region: "
									+ incentiveElasticityModelDTO.getRegion()
									+ "****** incentive: " + incentive);
					continue;
				}
			}

			// Pick best dataset.
			incentiveElasticityModelDTO = pickbest.bestdataset(dataOutlier,
					incentiveElasticityModelDTO);
			DataFrame bestDataSet = incentiveElasticityModelDTO
					.getBestDataSet();
			if (bestDataSet == null) {
				mrmDevLogger
						.info(" ****** The output map from the pick best dataset is empty ****** ");
				continue;
			}

			incentiveElasticityModelDTO.setBestDataSet(bestDataSet);

			// Pickup Best dataset.
			pickupBestDataSet(incentiveElasticityModelDTO, incentive,
					sumelasticity, elasticitycount);
		}
	}

	/**
	 * 
	 * This method calls the bayesian module and find the elasticity value.
	 * Finally output write to the output table.
	 * 
	 * @param bestdat
	 * @param incentiveElasticityModelDTO
	 * @param series
	 * @param regionCode
	 * @param incentive
	 * @param independentVariable
	 * @param sumelasticity
	 * @param elasticitycount
	 * @return incentiveElasticityModelDTO
	 * @throws MRMBaseException
	 */
	private IncentiveElasticityModelDTO pickupBestDataSet(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO,
			String incentive, Double sumelasticity, int elasticitycount)
			throws MRMBaseException {
		// Calling the final elasticity using bayesian
		BasianFinalElasticity finalElasticity = new BasianFinalElasticity();
		double elsticity = finalElasticity.finalelasticity(
				incentiveElasticityModelDTO.getBestDataSet(),
				incentiveElasticityModelDTO.getParameterMap());

		if (!incentiveElasticityModelDTO.getRegionValues().contains("NATIONAL")) {
			HiveContext hiveContext = incentiveElasticityModelDTO
					.getHiveContext();
			// Creating a output number to store the rowid
			hiveContext
					.sql("CREATE TABLE IF NOT EXISTS mrm_execution_number (Rowid INT)");
			// Creating a output table to store the final elasticity
			hiveContext
					.sql("CREATE TABLE IF NOT EXISTS mrm_output_table (Rowid INT ,UserID INT, Series STRING, Region INT, Incentive_type STRING, Elasticity DOUBLE,ElasticityDriver String, Execution_date STRING)");

			// Executuion number is the rowid which increments for
			// every spark submit
			DataFrame executionNumberDf = hiveContext
					.sql("select count(*) from mrm_execution_number");

			Long executionNumber = executionNumberDf.collect()[0].getLong(0) + 1;
			DataFrame executionumberrecord = hiveContext.sql("select "
					+ executionNumber + " as Rowid ");
			executionumberrecord.saveAsTable("mrm_execution_number",
					SaveMode.Append);

			// Adding Current date in simple date format
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");

			// Creating a output dataframe with respective column
			// names
			DataFrame outRecord = hiveContext.sql("select " + executionNumber
					+ " as Rowid," + incentiveElasticityModelDTO.getUserID()
					+ " as userid, '" + incentiveElasticityModelDTO.getSeries()
					+ "' as Series, " + incentiveElasticityModelDTO.getRegion()
					+ " as Region, '" + incentive + "' as Incentive_type, "
					+ elsticity + " as Elasticity, '"
					+ incentiveElasticityModelDTO.getIndependentString()
					+ "' as ElasticityDriver, '"
					+ simpleDateFormat.format(new Date())
					+ "' as Execution_date");

			// save that data frame into a hive table
			outRecord.saveAsTable("mrm_output_table", SaveMode.Append);
			java.util.Date date = new java.util.Date();
			Timestamp t2 = new Timestamp(date.getTime());

			hiveContext
					.sql("INSERT INTO irm_data_set select tc.* from ( select "
							+ executionNumber
							+ ",'"
							+ incentiveElasticityModelDTO.getUserID()
							+ "@"
							+ t2
							+ "','Incetive MRM Model', 'irm pm residual value', 'perdictive model-rv', 'forecast', 'internal','tbd', 'transaction', 'y','QUARTERLY','"
							+ t2 + "') tc ");
			Timestamp end_time = new Timestamp(date.getTime());
			hiveContext
					.sql("INSERT INTO table runstats SELECT t.* FROM   (SELECT 'Incentive MRM  Model', 'RAW LAYER', '"
							+ incentiveElasticityModelDTO.getStart_time()
							+ "', '"
							+ end_time
							+ "',  '',  '',  '',  '',  '"
							+ incentiveElasticityModelDTO.getUserID()
							+ "','"
							+ executionNumber
							+ "',  '"
							+ incentiveElasticityModelDTO.getUserID()
							+ "',  '"
							+ incentiveElasticityModelDTO.getUserID()
							+ "',  '',  '',  '') t");

		}
		elasticitycount++;
		sumelasticity += elsticity;

		// Setting elasticity count and sum of elasticity.
		incentiveElasticityModelDTO.setElasticitycount(elasticitycount);
		incentiveElasticityModelDTO.setSumelasticity(sumelasticity);
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method reads the share incentive XLS properties and incentive share
	 * XLS properties.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 * @throws IOException
	 * @throws MRMNoQueryParametersFoundException
	 */
	private IncentiveElasticityModelDTO readPropertiesData(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws IOException, MRMNoQueryParametersFoundException {
		Properties constantproperties = incentiveElasticityModelDTO
				.getQueryproperties();
		if (constantproperties.isEmpty()) {
			throw new MRMNoQueryParametersFoundException(
					MRMIndexes.getValue("EMPTY_QUERY_PARAMS"));
		} else {
			// Read double values.
			String[] doublesProperty = constantproperties
					.getProperty("doubles").split(",");
			double[] doubles = new double[doublesProperty.length];
			for (int i = 0; i < doublesProperty.length; i++) {
				doubles[i] = Double.parseDouble(doublesProperty[i]);
			}
			incentiveElasticityModelDTO.setDoubles(doubles);

			// adding default parameters to the code
			String[] shareInctvExclProperty = constantproperties.getProperty(
					"shareInctvExcl").split(",");
			double[] shareInctvExcl = new double[shareInctvExclProperty.length];
			for (int i = 0; i < shareInctvExclProperty.length; i++) {
				shareInctvExcl[i] = Double
						.parseDouble(shareInctvExclProperty[i]);
			}

			String[] inctvRatioExclProperty = constantproperties.getProperty(
					"inctvRatioExcl").split(",");
			double[] inctvRatioExcl = new double[inctvRatioExclProperty.length];
			for (int i = 0; i < inctvRatioExclProperty.length; i++) {
				inctvRatioExcl[i] = Double
						.parseDouble(inctvRatioExclProperty[i]);
			}

			// create a double array
			double[] doubles2 = new double[doubles.length * 2];
			for (int i = 0; i < doubles.length; i++) {
				doubles2[i] = doubles[i];
				doubles2[i + doubles.length] = 1 - doubles[i];
			}

			// sort the arrays list
			Arrays.sort(doubles2);

			incentiveElasticityModelDTO.setShareInctvExcl(shareInctvExcl);
			incentiveElasticityModelDTO.setInctvRatioExcl(inctvRatioExcl);
			incentiveElasticityModelDTO.setDoubles2(doubles2);
		}

		return incentiveElasticityModelDTO;
	}

}
