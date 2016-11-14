package com.toyota.analytix.mrm.msrp.dataPrep;

import static com.toyota.analytix.common.spark.SparkRunnableModel.hiveContext;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.common.util.QueryProperties;

public class HiveDataLoader extends DataLoader {

	private static final Logger logger = Logger.getLogger(HiveDataLoader.class);

	private String userid;
	private String model;
	private String baseSchema;
	private String integratedSchema;

	// Queries
	private String inputquery2;
	private String timestampquery;
	private String parameter_table;
	private String series_dataframe;
	private String series_table;
	private String region_table;
	private String inputquery;
	
	public static String USERID = "userid";
	public static String DATABASE_BASE_SCHEMA = "database_base_schema";
	public static String DATABASE_INTEGRATED_SCHEMA = "database_integrated_shema";
	public static String MODEL = "model";

	//public static Timestamp timestamp;

	protected HiveDataLoader() {
		userid = getRequiredProperty(USERID);
		baseSchema = getRequiredProperty(DATABASE_BASE_SCHEMA);
		integratedSchema = getRequiredProperty(DATABASE_INTEGRATED_SCHEMA);
		model = getRequiredProperty(MODEL);

		inputquery2 = replaceParamsInQuery(getRequiredProperty("inputquery2"));
		timestampquery = replaceParamsInQuery(getRequiredProperty("timestampquery"));
		parameter_table = replaceParamsInQuery(getRequiredProperty("parameter_table"));
		series_dataframe = replaceParamsInQuery(getRequiredProperty("series_dataframe"));
		series_table = replaceParamsInQuery(getRequiredProperty("series_table"));
		region_table = replaceParamsInQuery(getRequiredProperty("region_table"));
		inputquery = replaceParamsInQuery(getRequiredProperty("inputquery"));
	}

	@Override
	public Timestamp getLatestTimestamp() {
		logger.debug("Timestamp Query:" + timestampquery);
		Timestamp latestTimestamp  = null;
		try{
		// Get the latest time stamp.
			latestTimestamp = (Timestamp) hiveContext.sql(timestampquery).collect()[0].get(0);
		} catch (Exception ex){
			logger.error ("Could not obtain timestamp using query: " + timestampquery + "\nDetails:", ex);
			throw new AnalytixRuntimeException ("Could not obtain timestamp using query: " + timestampquery + "\nDetails:" + ex.getMessage());
			
		}
		//this.timestamp = latestTimestamp;
		logger.debug("TimeStamp from Timestamp Query:" + latestTimestamp);
		return latestTimestamp;
	}
	
	private String getRequiredProperty(String property) {
		String value = QueryProperties.getQueryValue(property);
		if (StringUtils.isEmpty(value)) {
			throw new AnalytixRuntimeException("Required property " + property + "  does not exist in properties file");
		}
		return value;
	}

	@Override
	public DataFrame loadSeriesData() {
		// Get the series data frame.
		DataFrame seriesDataframe = hiveContext.sql(series_table);
		return seriesDataframe;
	}

	@Override
	public DataFrame loadFilterData() {
		logger.debug("Filter Data Query:" + timestampquery);

		// Get the series data frame.
		DataFrame seriesDataframe = hiveContext.sql(series_dataframe).filter("create_ts = CAST('" + super.latestTimeStamp
				+ "' as TIMESTAMP) and create_user_id  = '" + userid + "' and consumer_system_nm = '" + model + "'");
		return seriesDataframe;
	}

	@Override
	public DataFrame loadRegionData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataFrame loadMSRPData() {
		DataFrame inputdata = null;

		//flag to control data population (running the big query) into the integration layer table
		String populateData = QueryProperties.getQueryValue("skip-driver-table-population");
		boolean skipDataPopulation = false;
		if (StringUtils.isNotBlank(populateData) && populateData.equalsIgnoreCase("true")){
			logger.warn("************ Setting the skip-driver-table-population to true. "
					+ "Please make sure that this is intended. ***************");
			skipDataPopulation = true;
		}
		
		if (!skipDataPopulation) {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String timestampStr = simpleDateFormat.format(new Date());
			inputquery2 = inputquery2.replace("${start_date}", timestampStr);

			// Get the input table and set the input table.
			logger.debug("Loading data with Query:" + inputquery2);

			try {
				// this creates a temp table
				hiveContext.sql(inputquery2);
			} catch (Exception e) {
				logger.error("Unable to get the MRM input data");
				throw e;
			}
		}
		logger.debug("loading data frame with query:" + inputquery);
		inputdata = hiveContext.sql(inputquery);
		if (null == inputdata) {
			throw new AnalytixRuntimeException("No input data");
		}

		return inputdata;
	}

	public void saveOutput(String userid, String series, String regionCode, String incentive, Double elsticity,
			String indep_String) {
		hiveContext.sql("CREATE TABLE IF NOT EXISTS mrm_execution_number (Rowid INT)");

		// Creating a output table to store the final elasticity
		hiveContext.sql(
				"CREATE TABLE IF NOT EXISTS mrm_output_table (Rowid INT ,UserID INT, Series STRING, Region STRING, Incentive_type STRING, Elasticity DOUBLE,ElasticityDriver String, Execution_date STRING)");

		// Executuion number is the rowid which increments for every spark
		// submit
		DataFrame executionNumberDf = hiveContext.sql("select count(*) from mrm_execution_number");

		Long executionNumber = executionNumberDf.collect()[0].getLong(0) + 1;

		DataFrame executionumberrecord = hiveContext.sql("select " + executionNumber + " as Rowid ");
		executionumberrecord.saveAsTable("mrm_execution_number", SaveMode.Append);

		// Adding Current date in simple date format
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// Creating a output dataframe with respective column names
		DataFrame outRecord = hiveContext.sql("select " + executionNumber + " as Rowid," + userid + " as userid, "
				+ series + " as Series, " + regionCode + " as Region, '" + incentive + "' as Incentive_type, "
				+ elsticity + " as Elasticity, '" + indep_String + "' as ElasticityDriver, '"
				+ simpleDateFormat.format(new Date()) + "' as Execution_date");

		// save that data frame into a hive table
		outRecord.saveAsTable("mrm_output_table", SaveMode.Append);
	}

	@Override
	protected DataFrame loadParameterMap() {
		// Reading the parameter table data.
		DataFrame parameterTable = hiveContext.sql(parameter_table);

		// Checking properties from the parameter table.
		if (parameterTable == null || parameterTable.count() == 0) {
			throw new AnalytixRuntimeException(
					"Could not load parameters from parameter table using " + parameter_table);
		}
		return parameterTable;
	}

	private String replaceParamsInQuery(String input) {
		return input.replace("${userid}", userid).replace("${model}", model)
				.replace("${database_base_schema}", baseSchema)
				.replace("${database_integrated_schema}", integratedSchema);
	}
	
	public String toString(){
		return "inputQuery:" + inputquery+
				"\ninputquery2:"+ inputquery2 +
				"\nparameter_table:" + parameter_table +
				"\nregion_table:" + region_table+
				"\nseries_table:" + series_table +
				"\ntimestampquery:" + timestampquery+
				"\nseries_dataframe:" + series_dataframe;
	}
	
	public static void main(String[] args){
		HiveDataLoader h = new HiveDataLoader();
		System.out.println("queries:" + h);
	}
}
