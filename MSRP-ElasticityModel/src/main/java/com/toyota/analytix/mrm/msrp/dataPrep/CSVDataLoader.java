package com.toyota.analytix.mrm.msrp.dataPrep;

import static com.toyota.analytix.common.spark.SparkRunnableModel.hiveContext;

import java.sql.Timestamp;

import org.apache.spark.sql.DataFrame;

import com.toyota.analytix.common.util.QueryProperties;

public final class CSVDataLoader extends DataLoader {

	private static final String SERIES_DATA = "series_data";
	//private static final String REGION_DATA = "region_data";
	private static final String MSRP_DATA = "MSRP_data";
	private static final String PARAM_FILE = "parameter_file";
	private static final String FILTER_FILE = "filter_file";

	private String seriesFile;
	//private String regionsFile;
	private String MSRPFile;
	private String paramFile;
	private String filterFile;

	protected CSVDataLoader() {
		seriesFile = QueryProperties.getQueryValue(SERIES_DATA);
		//regionsFile = QueryProperties.getQueryValue(REGION_DATA);
		MSRPFile = QueryProperties.getQueryValue(MSRP_DATA);
		paramFile = QueryProperties.getQueryValue(PARAM_FILE);
		filterFile = QueryProperties.getQueryValue(FILTER_FILE);
	}

	@Override
	protected DataFrame loadSeriesData() {
		String createSql = " make_nm string, series_fuel_nm string, series_id int, make_id int, car_truck string, series string, fcst_run_flg string, msrp_mrm_run_flg string, inctv_mrm_run_flg string, agg_series_id string, ui_flg string, hybrid_flg string, gi_flg string, fle_tac_flg string ";
		String table = "series_data";
		hiveContext.sql("drop table if exists " + table);
		return loadFileIntoDataFrame(table, createSql, seriesFile);
	}

	@Override
	public DataFrame loadRegionData() {
		return null;
//		String createSql = " make_id int, region_code int, name string, city_code1 string, city_code2 string ";
//		String table = "regions_data";
//		return loadFileIntoDataFrame(table, createSql, regionsFile);
	}

	@Override
	public DataFrame loadMSRPData() {
		String table = "msrp_mrm_input";
		hiveContext.sql("drop table if exists " + table);
		//String createSql = " make_nm string, series_fuel_nm string, car_truck string, model_year string, region_code string, month string, business_month string, in_market_stage string, months_in_market double, base_msrp double, msrp_comp_ratio double, daily_sales string, sales_share double, incentive double, incentive_comp_ratio double, comp_msrp string, comp_incentive double, cftp double, life_cycle_position string, years_since_major_change string, major_change string, minor_change string, comp_life_cycle_position string, comp_years_since_major_change string, comp_major_change string, comp_major_change_plus_1 string, japan_tsunami string, gas_price double, gas_price_chg_pct double ";
		String createSql = "make_nm String,series_fuel_nm String,car_truck String,model_year String,month String,business_month String,in_market_stage String,months_in_market Double,daily_sales Double,vdw_sales Double,pin_sales Double,base_msrp Double,comp_msrp Double,years_since_major_change Double,competitor_yrs_since_major_change Double,incentive Double,sales_share Double,incentive_comp_ratio Double,msrp_comp_ratio Double,comp_incentive Double,cftp Double,life_cycle_position String,major_change Double,minor_change Double,comp_life_cycle_position String,comp_major_change Double,comp_major_change_plus_1 Double,japan_tsunami Double,gas_price Double,gas_price_chg_pct Double,msrp_sales_prod Double,comp_msrp_sales_prod Double,incentive_sales_prod Double,comp_incentive_sales_prod Double,competitor_yrs_since_major_change_sales_prod Double,cftp_sales_prod Double,create_ts String";
		return loadFileIntoDataFrame(table, createSql, MSRPFile);
	}

	// private DataFrame loadFileIntoDataFrame(String pathToFile) {
	// HashMap<String, String> options = new HashMap<String, String>();
	// options.put("header", "true");
	// options.put("inferSchema", "true");
	//
	// // Path to the file
	// options.put("path", pathToFile);
	//
	// // Use DataBricks csv provider to load csv
	// DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
	// return df;
	// }

	private DataFrame loadFileIntoDataFrame(String tableName, String createSql, String pathToFile) {
		hiveContext.sql("CREATE TABLE IF NOT EXISTS " + tableName + "(" + createSql
				+ ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY \"{\" LINES TERMINATED BY '\\n' STORED AS TEXTFILE tblproperties('skip.header.line.count'='1') ");

		hiveContext.sql("LOAD DATA LOCAL INPATH '" + pathToFile + "' OVERWRITE INTO TABLE " + tableName);

		// Creating a data frame to save the input data from hive
		DataFrame input_data_frame = hiveContext.sql("select * from " + tableName );

		return input_data_frame;

	}

	@Override
	protected DataFrame loadParameterMap() {
		String createSql = " parameter_name String, description String, consumer_system String, granularity String, granularity_value String, parameter_value String, start_date String, end_date String, create_source String, create_user_id String";
		String table = "irm_parameter";
		hiveContext.sql("drop table if exists " + table);

		DataFrame df = loadFileIntoDataFrame(table, createSql, paramFile);
		df = df.filter("consumer_system = 'MSRP_MRM'");
		return df;
	}

	@Override
	protected Timestamp getLatestTimestamp() {
		// DateFormat format = new SimpleDateFormat("");
		String timestampStr = "2016-05-25 03:01:23";
		return Timestamp.valueOf(timestampStr);
	}

	@Override
	protected DataFrame loadFilterData() {
		String table = "msrp_mrm_filter";
		hiveContext.sql("drop table if exists " + table);
		String createSql = " filter_field String,filter_value String,create_user_id String,consumer_system_nm String,create_ts String";
		return loadFileIntoDataFrame(table, createSql, filterFile);

	}

}
