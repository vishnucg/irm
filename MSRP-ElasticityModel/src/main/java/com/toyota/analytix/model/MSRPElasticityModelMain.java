package com.toyota.analytix.model;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.types.DataTypes;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.common.spark.SparkRunnableModel;
import com.toyota.analytix.common.util.CollectionUtil;
import com.toyota.analytix.common.util.MRMUtil;
import com.toyota.analytix.common.util.QueryProperties;
import com.toyota.analytix.common.util.SimpleTimer;
import com.toyota.analytix.mrm.msrp.bayesian.BayesianFinalElasticity;
import com.toyota.analytix.mrm.msrp.bestDataset.PickbestDataset;
import com.toyota.analytix.mrm.msrp.bestDataset.PickbestResult;
import com.toyota.analytix.mrm.msrp.dataPrep.DataLoader;
import com.toyota.analytix.mrm.msrp.dataPrep.HiveDataLoader;
import com.toyota.analytix.mrm.msrp.dataPrep.ModelParameters;

public class MSRPElasticityModelMain extends SparkRunnableModel {

	//static Date startTime;
	
	public static void main(String[] args) throws Exception{
		SimpleTimer t = new SimpleTimer();

		Properties props = readArgumentsToProperties(args);

		//System.getProperty ("runProperties")
		MSRPElasticityModelMain m = new MSRPElasticityModelMain(props.getProperty(HiveDataLoader.MODEL));
		m.logger.info("Starting MSRP Model");

		m.processModel(props);
		m.logger.info("End MSRP Model. Total Time "  + t.endTimer() + " millis");
	}

	public MSRPElasticityModelMain (String appName){
		super(appName);
	}

	public void processModel(Properties props) throws Exception{
		logger.info("Properties:" + CollectionUtil.debugPrintMap(props));
		
		hiveContext.sql("use " +
			props.getProperty(HiveDataLoader.DATABASE_BASE_SCHEMA));
		
		// Step 1. Data Preparation
		DataLoader dataLoader = DataLoader.loadData(props);
		logger.info("****************** Loaded the Data ********************");

		DataFrame seriesData = dataLoader.getSeriesData();
		//DataFrame regionData = dataLoader.getRegionData();
		DataFrame filterData = dataLoader.getFilterData();
		logger.debug("Filter Data:");
		filterData.show();
		
		DataFrame MSRPData = dataLoader.getMSRPData();
		ModelParameters modelParameters = getModelParameters(dataLoader.getParameters());
		if (modelParameters == null){
			logger.error("Model Parameters are null. Exiting... ");
			return;
		}
		logger.debug("Series Data:");
		seriesData.show();
		
		logger.info("model started :::: ");

		//Do the national aggregation
		
		//get National MSRPData
		logger.info ("Total rows in all series data :"+ MSRPData.count() );
		SimpleTimer t = new SimpleTimer();

		DataFrame nationalMSRPData = MSRPData;
//		boolean skipAggregation = false;
//		//if region-aggregation flag is not explicitly set, we do region aggregation to national
//		if (props.get("region-aggregation") != null && props.get("region-aggregation").equals("skip-aggregation")){
//			skipAggregation = false;
//		}
//		if (skipAggregation){
//			nationalMSRPData = MSRPData;
//		}else{
//			logger.info ("Starting National Aggregation :"  );
//			nationalMSRPData = DataNationalizationRunner.aggregateNationalData(MSRPData);
//		}
		
//		logger.info ("End National Aggregation. National Data Size(rows):"+ nationalMSRPData.count()  + ". Time taken(ms):" +t.endTimer());
		//get series list to process
		List<String> seriesList = getSeriesToRun(seriesData, filterData);
		String regionString = getRegionToRun(filterData);
		//if region string in not there, assume national
		if (StringUtils.isBlank(regionString)){
			regionString = "99";
		}
		
		logger.info ("Running the model for :" + Arrays.toString(seriesList.toArray()));
		
		int runId =1;
		//iterating through series 
		for (String seriesName : seriesList) {
			int minrequiredNumModelYears = 2;
			int minNumData = 2;
			t = new SimpleTimer();

			try {
				logger.info("Series DataFrame before Filter:");
				nationalMSRPData.show();
				
				logger.info("Series DataFrame filter for Lexus Series For DEBUG PURPOSES ONLY:");
				DataFrame df = nationalMSRPData.filter("make_nm='Lexus'");
				df.show();

				logger.info("Series DataFrame to be considered:");
				df = nationalMSRPData.filter("series_fuel_nm = '" + seriesName );
				df.show();

				logger.info("Data Filter:" + "series_fuel_nm = '" + seriesName + "' and region_code='" + regionString + "'");
				// Filter the data for series
				DataFrame seriesNationalData = df;
//						nationalMSRPData
//						.filter("series_fuel_nm = '" + seriesName + "' and region_code='" + regionString + "'");
				logger.info("***********Data for SERIES = " + seriesName + "*********");
				if (seriesNationalData == null || seriesNationalData.count() == 0) {
					logger.info("**** No Data for SERIES = " + seriesName + " and region code:" + regionString
							+ ". Skipping");
					continue;
				} else if (seriesNationalData.count() < minNumData) {
					logger.info("**** Not enough rows for SERIES = " + seriesName + " and region code:" + regionString
							+ ". Skipping the series");
					continue;
				}

				// Needs to be done in order to avoid 1.3.0 bug
				// resolved attributes sales_share missing from
				// https://www.mail-archive.com/search?l=user@spark.apache.org&q=subject:%22dataframe+can+not+find+fields+after+loading+from+hive%22&o=newest&f=1
				seriesNationalData = seriesNationalData.sqlContext().createDataFrame(seriesNationalData.rdd(),
						seriesNationalData.schema());

				logger.info("Series MSRP Data Size:" + seriesNationalData.count());
				long dataNumYears = seriesNationalData.groupBy("model_year").count().count();
				logger.debug("SeriesId: " + seriesName + " Time taken(groupby) :" + t.endTimer() + "millis.");
				// if we don't have more than two years of data, don't process
				// the series
				if (dataNumYears < minrequiredNumModelYears) {
					logger.info("**** Not enough number of years of data for series_fuel_nm = " + seriesName
							+ " and region_cd=" + regionString + ". Skipping");
					continue;
				}

				// run model for series
				t.resetTimer();
				runModelForSeries(seriesName, regionString, seriesNationalData, modelParameters, props, runId);
				logger.debug("Series: " + seriesName + " Region:" + regionString
						+ ". Time taken for the series run(without model year grouping):" + t.endTimer() + "millis.");
			} catch (Exception ex) {
				logger.error("\n************************************ \n "
						+ "Error occured in running the model for Series:" + seriesName + ". Region:" + regionString +". Skipping the series.. Details below \n"
						+ "************************************", ex);
			}

			runId++;
		}
		logger.info("Finished processing  series: " + Arrays.toString(seriesList.toArray()));
		sc.stop();
	}


	private void runModelForSeries(String seriesName, String regionString, DataFrame seriesNationalData,
			ModelParameters modelParameters, Properties props, int runId) {
		double elasticity = 0; 
		String indepVariables = "";
		long dataSize = 0;
		try {
			seriesNationalData.show();

			// add new column DEPENDENT_VAR same as SALES_SHARE
			seriesNationalData = seriesNationalData.withColumn("dependent_variable",
					seriesNationalData.col("sales_share"));

			// add new column SALES_SHARE_OUTLIER
			seriesNationalData = seriesNationalData.withColumn("sales_share_outlier",
					new Column(new Literal(100, DataTypes.IntegerType)));

			// add new column MSRP_RATIO_OUTLIER
			seriesNationalData = seriesNationalData.withColumn("msrp_ratio_outlier",
					new Column(new Literal(100, DataTypes.IntegerType)));

			/**
			 * DataPrep
			 */
			logger.info("***********Added SALES_SHARE_OUTLIER, MSRP_RATIO_OUTLIER*********");
			logger.debug("Before Factorization:");
			seriesNationalData.show();
			// Enumerate String Valued rows that are needed for regression
			seriesNationalData = MRMUtil.enumerateColumns(seriesNationalData, "in_market_stage", "life_cycle_position",
					"comp_life_cycle_position");

			logger.info("*********** After Factorization *********");
			seriesNationalData.show();
			seriesNationalData.printSchema();

			// 1. Run through the different outlier windows and pick the best
			// dataset
			PickbestDataset pickbest = new PickbestDataset();
			PickbestResult pickbestResult = pickbest.bestDataSet(seriesNationalData, seriesName, modelParameters);
			DataFrame bestdat = pickbestResult.getResultDataFrame();

			if (bestdat == null) {
				throw new AnalytixRuntimeException(
						" Best Dataset not found for SERIES_ID = " + seriesName + ". Skipping the series");
			}
			dataSize = bestdat.count();
			indepVariables = pickbestResult.getFormula();

			// 2. BAYESIAN
			// get prior mean from previous runs
			Double priorMean = getPriorFromPreviousRun(seriesName, regionString, props, modelParameters);
			if (priorMean != null) {
				modelParameters.resetPriorMean(priorMean);
			}
			logger.info("****  Starting Bayesian for series Id:" + seriesName + ".");
			SimpleTimer bayesTimer = new SimpleTimer();
			BayesianFinalElasticity finalelas = new BayesianFinalElasticity();
			elasticity = 0.0;
			try {
				elasticity = finalelas.finalelasticity(bestdat, seriesName, modelParameters);
			} catch (AnalytixRuntimeException e) {
				logger.error("problem with finding final elasticity for for seried_id = " + seriesName + " region_cd:"
						+ regionString, e);
			}

			logger.info(
					"Bayesian Elasticity for series: " + seriesName + " region:" + regionString + " is" + elasticity);

			logger.info("****  End Bayesian for series Id:" + seriesName + ". Time taken: " + bayesTimer.endTimer());
		} catch (Exception ex) {
			logger.error(
					"Error in model for series" + seriesName + " region:" + regionString , ex);
			logger.warn(
					"Assuming prior elasticity as the elasticity");
			elasticity = modelParameters.getPriorMean();
		}
		if (elasticity == 0){
			logger.warn("Bayesian returned elasticity as 0. Assuming elasticity as prior.");
			elasticity = modelParameters.getPriorMean();
		}

		String userId = props.getProperty(HiveDataLoader.USERID);
		saveoutput(props.getProperty(HiveDataLoader.DATABASE_BASE_SCHEMA), userId, runId, seriesName, regionString,
				elasticity, indepVariables, dataSize);

	}
	
	private ModelParameters getModelParameters(DataFrame parameters) {
		Map<String, String> parameter_map = new HashMap<>();
		for (Row parameter : parameters.collect()) {
			parameter_map.put(parameter.getString(0).toLowerCase().trim(), parameter.getString(5).trim());
		}

		parameter_map.put("prior.adj.factor.no.best.model", "2");
		parameter_map.put("dep.var", "dependent_variable");
		parameter_map.put("incl.var", "msrp_comp_ratio");
		
		logger.info("Parameters for the model\n:" + MRMUtil.debugPrintMap(parameter_map));
		
		ModelParameters m = ModelParameters.getInstance(parameter_map);
		return m;
	}

	public void saveoutput(String schemaName, String userid, int runId, String series, String regionCode,
			Double elsticity, String indepString, long numDataPoints) {
		hiveContext.sql("CREATE TABLE IF NOT EXISTS mrm_execution_number (Rowid INT)");

		// Creating a output table to store the final elasticity
		// hiveContext
		// .sql("CREATE TABLE IF NOT EXISTS mrm_output_table (Rowid INT ,UserID
		// INT, Series STRING, Region STRING, Incentive_type STRING, Elasticity
		// DOUBLE,ElasticityDriver String, Execution_date STRING)");

		// Executuion number is the rowid which increments for every spark
		// submit
		DataFrame executionNumberDf = hiveContext.sql("select count(*) from mrm_execution_number");

		Long executionNumber = executionNumberDf.collect()[0].getLong(0) + 1;

		DataFrame executionumberrecord = hiveContext.sql("select " + executionNumber + " as Rowid ");
		executionumberrecord.saveAsTable("mrm_execution_number", SaveMode.Append);
		// String createTable = "Create Table IF NOT EXISTS "
		// + schemaName + ".raw_irm_pm_msrp_elasticity"
		// + " ( raw_data_set_row_id String, "
		// + " data_set_id String, raw_series_fuel_nm String, raw_region_cd String,"
		// + " elasticity_type String, elasticity_qty String, "
		// + " msrp_elasticity_driver String, num_hist_data_pts String,
		// effective_dt String )";
		//
		// logger.debug("Create table statement for insert: " + createTable);
		// hiveContext.sql(createTable);

		// Adding Current date in simple date format
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// Creating a output dataframe with respective column names
		String timestampStr = simpleDateFormat.format(new Date());
		String insertStatement = "select '" + userid + "@" + timestampStr + "#" + runId + "' as raw_data_set_row_id,'"
				+ executionNumber + "' as data_set_id, '" + series + "' as raw_series_nm, '" + regionCode
				+ "' as raw_region_cd, '" + "msrp" + "' as elasticity_type, '" + elsticity + "' as elasticity_qty, '"
				+ indepString + "' as msrp_elasticity_driver, '" + numDataPoints + "' as num_historical_data_pts ,'"
				+ timestampStr + "' as effective_date";
		logger.debug("insert:" + insertStatement);
		DataFrame outRecord = hiveContext.sql(insertStatement);

		// save that data frame into a hive table
		outRecord.saveAsTable("raw_irm_pm_msrp_elasticity", SaveMode.Append);

	}

	private List<String> getSeriesToRun(DataFrame seriesData, DataFrame filterData) {
		List<String> seriesList = new ArrayList<String>();
		if (filterData != null) {
			// iterating through series
			for (Row seriesRow : filterData.collect()) {
				// from filter table
				if (seriesRow.getString(0).equals("MAKE_NM,SERIES_FUEL_NM,GRADE_NM")) {
					seriesList.add(seriesRow.get(1).toString().split(",")[1]);
				}
			}
		}
		//if series list is empty or contains ALL then run for all models
		if (seriesList.isEmpty() || seriesList.contains("ALL")) {
			seriesList.clear();
			for (Row r:seriesData.collect()){
				seriesList.add(r.getString(0));
			}
		}
		return seriesList;
	}
	
	private String getRegionToRun(DataFrame filterData) {
		String regionCode = null;
		if (filterData != null) {
			// iterating through series
			for (Row seriesRow : filterData.collect()) {
				// from filter table
				if (seriesRow.getString(0).equals("REGION_CODE")) {
					regionCode = seriesRow.get(1).toString();
					break;
				}
			}
		}
		return regionCode;
	}
	
	public static Properties readArgumentsToProperties(String[] args) {
		Properties props = new Properties();
		String userid = null;
		String database = null;
		String model = null;
		String todatabase = null;
		if (args == null || args.length < 5) {
			throw new AnalytixRuntimeException(" Arguments missing. Model needs five arguments in this order:"
					+ " <userid> <model> <base_database_schema_name> <integrated_database_schema_name> <query_properties_file_path>. Exiting...");
		}
		userid = args[0];
		model = args[1];
		database = args[2];
		todatabase = args[3];
		props.setProperty(HiveDataLoader.MODEL, model);
		props.setProperty(HiveDataLoader.USERID, userid);
		props.setProperty(HiveDataLoader.DATABASE_BASE_SCHEMA, database);
		props.setProperty(HiveDataLoader.DATABASE_INTEGRATED_SCHEMA, todatabase);
		props.setProperty(HiveDataLoader.DATABASE_INTEGRATED_SCHEMA, todatabase);
		props.setProperty(QueryProperties.QUERY_PROPS, args[4]);
		
		if (args.length == 6) {
			props.setProperty("region-aggregation", args[5]);
		}
		
		if (args.length == 7) {
			props.setProperty("dataloader", args[6]);
		}

		return props;
	}
	
	private Double getPriorFromPreviousRun(String seriesName, String regionCode, Properties properties,
			ModelParameters modelParameters) {
		Date priordate = null;
		double priormean = 0;
		String prior_mean_param = null;
		Timestamp priortimestamp = null;
		Timestamp t1 = null;
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		prior_mean_param = QueryProperties.getQueryValue("prior_mean_effective_date");
		logger.info("Date before adding utils::" + prior_mean_param);
		Timestamp d = null;
		try {
			priordate = format.parse(prior_mean_param);
			t1 = new java.sql.Timestamp(priordate.getTime());
			logger.info("Date after adding utils::" + t1);
		} catch (ParseException e) {
			throw new RuntimeException(
					"Could not parse the date for prior_mean_effective_date in parameters. It should be in yyyy-MM-dd format");
		}
		// Get the time stamp.
		if (prior_mean_param != null) {
			priortimestamp = t1;
		}
		if (priortimestamp == null) {
			String timestampFromLastRun = (String) hiveContext.sql(QueryProperties.getQueryValue("priortimestamp"))
					.filter("series_nm = '" + seriesName + "'").collect()[0].get(0);

			try {
				priortimestamp = new java.sql.Timestamp(format.parse(timestampFromLastRun).getTime());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		cal.setTime(priortimestamp);
		cal.add(Calendar.DATE, 1);
		priortimestamp = new java.sql.Timestamp(cal.getTimeInMillis());
		logger.info("timestamp :::: " + priortimestamp);
		DataFrame seriesDataframe = null;
		if (priortimestamp != null) {
			try {
				seriesDataframe = hiveContext.sql(QueryProperties.getQueryValue("prioroutputtable"))
						.filter(" effective_dt < '" + priortimestamp + "' and raw_series_nm  = '" + seriesName + "'");
			} catch (Exception e) {
				logger.info("Could not change date");
			}
		}
		if (seriesDataframe != null && seriesDataframe.count() > 0) {
			priormean = Double.parseDouble(seriesDataframe.collect()[0].getString(1));
		}
		if (priormean == 0) {
			priormean = modelParameters.getPriorMean();
		}
		return priormean;
	}
	
	private static String replaceParamsInQuery(String query, String userId, String model, String baseSchema, 
			String integratedSchema, String seriesName, String regionCode, String effectiveDate) {
		return query.replace("${userid}", userId).replace("${model}", model)
				.replace("${database_base_schema}", baseSchema)
				.replace("${database_integrated_schema}", integratedSchema)
				.replace("${series_fuel_nm}", seriesName)
				.replace("${region_cd}", regionCode)
				.replace("${effective_date}", effectiveDate);
	}
	

}
