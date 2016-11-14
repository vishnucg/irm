package com.toyota.analytix.mrm.msrp.dataPrep;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.opencsv.CSVReader;
import com.toyota.analytix.common.spark.SparkRunnableModel;

public class DataNationalizationRunner implements Serializable {

	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(DataNationalizationRunner.class);
	
	public static DataFrame aggregateNationalData(DataFrame regionalData) {
		regionalData.printSchema();
		logger.debug("Regional data: Number of rows:"+regionalData.count());
		regionalData.show();
		
		//Needs to be done in order to avoid 1.3.0 bug
		//resolved attributes sales_share missing from 
		//https://www.mail-archive.com/search?l=user@spark.apache.org&q=subject:%22dataframe+can+not+find+fields+after+loading+from+hive%22&o=newest&f=1
		regionalData = regionalData.sqlContext().createDataFrame(regionalData.rdd(), regionalData.schema());
//		String grpBy = QueryProperties.getQueryValue("national_group_by");
//		logger.debug("national_group_by value from query.properties: " + grpBy);
		
		//Region code and model year grouping
		DataFrame msrpSumProd = regionalData.groupBy(
				"make_nm",
				"series_fuel_nm"
				,"car_truck","model_year",
				"month","business_month"
				,"in_market_stage",
				"life_cycle_position", "create_ts"
		).agg(
				regionalData.col("make_nm"),
				regionalData.col("series_fuel_nm").as("series_fuel_nm"), 
				regionalData.col("car_truck").as("car_truck"),	
				regionalData.col("model_year").as("model_year"), 
				regionalData.col("month").as("month"), 
				regionalData.col("business_month").as("business_month"),
				regionalData.col("in_market_stage").as("in_market_stage"), 
				regionalData.col("life_cycle_position").as("life_cycle_position"), 
				org.apache.spark.sql.functions.avg("MONTHS_IN_MARKET").as("MONTHS_IN_MARKET"),
				org.apache.spark.sql.functions.avg("MAJOR_CHANGE").as("MAJOR_CHANGE"),
				org.apache.spark.sql.functions.avg("MINOR_CHANGE").as("MINOR_CHANGE"),
				org.apache.spark.sql.functions.avg("YEARS_SINCE_MAJOR_CHANGE").as("YEARS_SINCE_MAJOR_CHANGE"),
				org.apache.spark.sql.functions.avg("COMP_LIFE_CYCLE_POSITION").as("COMP_LIFE_CYCLE_POSITION"),
				org.apache.spark.sql.functions.avg("GAS_PRICE_CHG_PCT").as("GAS_PRICE_CHG_PCT"),
				org.apache.spark.sql.functions.avg("GAS_PRICE").as("GAS_PRICE"),
				org.apache.spark.sql.functions.avg("JAPAN_TSUNAMI").as("JAPAN_TSUNAMI"),
				org.apache.spark.sql.functions.sum("DAILY_SALES").as("DAILY_SALES"),
				org.apache.spark.sql.functions.sum("SALES_SHARE").as("SALES_SHARE"),
				org.apache.spark.sql.functions.avg("COMP_MAJOR_CHANGE").as("COMP_MAJOR_CHANGE"),
				org.apache.spark.sql.functions.avg("COMP_MAJOR_CHANGE_PLUS_1").as("COMP_MAJOR_CHANGE_PLUS_1"),
				org.apache.spark.sql.functions.sum("BASE_MSRP").as("MSRP_SALES_PROD_SUM"),
				org.apache.spark.sql.functions.sum("COMP_MSRP_SALES_PROD").as("COMP_MSRP_SALES_PROD_SUM"),
				org.apache.spark.sql.functions.sum("INCENTIVE_SALES_PROD").as("INCENTIVE_SALES_PROD_SUM"),
				org.apache.spark.sql.functions.sum("COMP_INCENTIVE_SALES_PROD").as("COMP_INCENTIVE_SALES_PROD_SUM"),
				org.apache.spark.sql.functions.sum("COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD").as("COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD_SUM"),
				org.apache.spark.sql.functions.sum("CFTP_SALES_PROD").as("CFTP_SALES_PROD_SUM"),
				org.apache.spark.sql.functions.sum("VDW_SALES").as("VDW_SALES_SUM"),
				org.apache.spark.sql.functions.sum("PIN_SALES").as("PIN_SALES_SUM")
//				,
//				regionalData.col("CREATE_TS").as("CREATE_TS")
		);
		
		logger.debug("after group by:");
		logger.debug("after group by: Number of rows:" + msrpSumProd.count());
		  msrpSumProd.show();
		  msrpSumProd = msrpSumProd.select(msrpSumProd.col("*"), 
		         msrpSumProd.col("MSRP_SALES_PROD_SUM").divide(msrpSumProd.col("VDW_SALES_SUM")).as("BASE_MSRP"),
		         msrpSumProd.col("CFTP_SALES_PROD_SUM").divide(msrpSumProd.col("VDW_SALES_SUM")).as("CFTP"), 
		         msrpSumProd.col("COMP_MSRP_SALES_PROD_SUM").divide(msrpSumProd.col("PIN_SALES_SUM")).as("COMP_MSRP"),
		         msrpSumProd.col("INCENTIVE_SALES_PROD_SUM").divide(msrpSumProd.col("PIN_SALES_SUM")).as("INCENTIVE"),
		         msrpSumProd.col("COMP_INCENTIVE_SALES_PROD_SUM").divide(msrpSumProd.col("PIN_SALES_SUM")).as("COMP_INCENTIVE"),
		         msrpSumProd.col("COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD_SUM").divide(msrpSumProd.col("PIN_SALES_SUM")).as("COMPETITOR_YRS_SINCE_MAJOR_CHANGE")
		  );
		  msrpSumProd.show();
		  
		  // ********** TODO: JOIN HERE ON SERIES_FUEL_NAME/BUSINES_MONTH *******************************
		 
		  // do final calculation to get values for MSRP_COMP_RATIO and INCENTIVE_COMP_RATIO
		  msrpSumProd =  msrpSumProd.selectExpr(
				  "make_nm",
				  "SERIES_FUEL_NM as series_fuel_nm",
				  "car_truck",
				  "model_year",
				  "99 as region_code",
				  "month",
				  "business_month",
				  "in_market_stage",
				  "months_in_market",
				  "daily_sales",
				  "base_msrp",
				  "comp_msrp",
				  "years_since_major_change",
				  "competitor_yrs_since_major_change",
				  "incentive",
				  "sales_share",
				  "INCENTIVE/COMP_INCENTIVE as incentive_comp_ratio",
				  "BASE_MSRP/COMP_MSRP as msrp_comp_ratio",
				  "comp_incentive",
				  "cftp",
				  "life_cycle_position",
				  "major_change",
				  "minor_change",
				  "comp_life_cycle_position",
				  "comp_major_change",
				  "comp_major_change_plus_1",
				  "japan_tsunami",
				  "gas_price",
				  "gas_price_chg_pct");
		  msrpSumProd.show();
		  
		  return msrpSumProd;
	}

	public DataFrame populateMSRPInputData() throws IOException {
		// ArrayList<String> mmiecsv = new ArrayList<String>();
		ArrayList<Object[]> mmiecsv = new ArrayList<Object[]>();
		DataFrame datagi = null;
		//queryproperties.load(DataNationalizationManager.class.getResourceAsStream("/Config.properties"));

		
		List<String[]> msrpInput = DataNationalizationRunner
				.parseCSVData("/Users//Downloads/msrp_data_dev.csv");
		int i = 0;
		for (String[] entry : msrpInput) {
			// if (entry.length ==
			// Integer.valueOf(queryproperties.getProperty("INPUT_QUERY_COLUMN_COUNT")))
			// {
			if (i==0){
				i++;
				continue;
			}
			MSRPInputEntry mmie = new MSRPInputEntry(entry);
			mmiecsv.add(mmie.toObjectArray());
			// mmiecsv.add(String.join(",", entry));
			// System.out.println(Arrays.toString(entry));
			// } else {
			// logger.debug(Arrays.toString(entry));
			// logger.debug("-- IGNORING INPUT ROW --\nInput query row does not
			// have expected column count: " + entry.toString());
			// }
			i++;
		}
		// System.exit(1);
		// System.out.println("Before spark parallelize");
		// JavaRDD<Object> mrmidd = SparkRunnableModel.sc.parallelize(mmiecsv);
		// System.out.println("After spark parallelize");

		// get the row list and the rdd
		List<Row> rows = new ArrayList<Row>();
		for (Object[] rowOfObjects : mmiecsv) {
			rows.add(RowFactory.create(rowOfObjects));
		}

		// create dataframe
		try {
			JavaRDD<Row> mrmidd = SparkRunnableModel.sc.parallelize(rows);
			//
			// JavaRDD<Row> dictrdd = mrmidd.map(new Function<Object, Row>() {
			// private static final long serialVersionUID =
			// -7114717641270412314L;
			//
			// public Row call(Object s) {
			//// System.out.println("s:" + s);
			// String[] array = s.split(",");
			//// System.out.println("Array:" + Arrays.toString(array));
			// return RowFactory.create((Object[])array);
			// }
			// });
			
  
			StructType newSchema = DataTypes.createStructType(
					new StructField[] { DataTypes.createStructField("MAKE_NM", DataTypes.StringType, false),
							DataTypes.createStructField("SERIES_FUEL_NM", DataTypes.StringType, false),
							DataTypes.createStructField("CAR_TRUCK", DataTypes.StringType, false),
							DataTypes.createStructField("MODEL_YEAR", DataTypes.StringType, false),
							DataTypes.createStructField("MONTH", DataTypes.StringType, false),
							DataTypes.createStructField("BUSINESS_MONTH", DataTypes.StringType, false),
							DataTypes.createStructField("IN_MARKET_STAGE", DataTypes.StringType, false),
							DataTypes.createStructField("MONTHS_IN_MARKET", DataTypes.DoubleType, false),
							DataTypes.createStructField("DAILY_SALES", DataTypes.DoubleType, false),
							DataTypes.createStructField("VDW_SALES", DataTypes.DoubleType, false),
							DataTypes.createStructField("PIN_SALES", DataTypes.DoubleType, false),
							DataTypes.createStructField("BASE_MSRP", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMP_MSRP", DataTypes.DoubleType, false),
							DataTypes.createStructField("YEARS_SINCE_MAJOR_CHANGE", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMPETITOR_YRS_SINCE_MAJOR_CHANGE", DataTypes.DoubleType, false),
							DataTypes.createStructField("INCENTIVE", DataTypes.DoubleType, false),
							DataTypes.createStructField("SALES_SHARE", DataTypes.DoubleType, false),
							DataTypes.createStructField("INCENTIVE_COMP_RATIO", DataTypes.DoubleType, false),
							DataTypes.createStructField("MSRP_COMP_RATIO", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMP_INCENTIVE", DataTypes.DoubleType, false),
							DataTypes.createStructField("CFTP", DataTypes.DoubleType, false),
							DataTypes.createStructField("LIFE_CYCLE_POSITION", DataTypes.StringType, false),
							DataTypes.createStructField("MAJOR_CHANGE", DataTypes.DoubleType, false),
							DataTypes.createStructField("MINOR_CHANGE", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMP_LIFE_CYCLE_POSITION", DataTypes.StringType, false),
							DataTypes.createStructField("COMP_MAJOR_CHANGE", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMP_MAJOR_CHANGE_PLUS_1", DataTypes.DoubleType, false),
							DataTypes.createStructField("JAPAN_TSUNAMI", DataTypes.DoubleType, false),
							DataTypes.createStructField("GAS_PRICE", DataTypes.DoubleType, false),
							DataTypes.createStructField("GAS_PRICE_CHG_PCT", DataTypes.DoubleType, false),
							DataTypes.createStructField("MSRP_SALES_PROD", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMP_MSRP_SALES_PROD", DataTypes.DoubleType, false),
							DataTypes.createStructField("INCENTIVE_SALES_PROD", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMP_INCENTIVE_SALES_PROD", DataTypes.DoubleType, false),
							DataTypes.createStructField("COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD",
									DataTypes.DoubleType, false),
							DataTypes.createStructField("CFTP_SALES_PROD", DataTypes.DoubleType, false),
							DataTypes.createStructField("create_ts", DataTypes.StringType, false) });

			datagi = SparkRunnableModel.hiveContext.createDataFrame(mrmidd, newSchema);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return datagi;
	}

	public static List<String[]> parseCSVData(String filePath) {
		CSVReader reader = null;
		List<String[]> data = null;
		try {
			reader = new CSVReader(new FileReader(filePath));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (reader != null) {
			try {
				data = reader.readAll();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return data.subList(1, data.size() - 1);
	}
}
