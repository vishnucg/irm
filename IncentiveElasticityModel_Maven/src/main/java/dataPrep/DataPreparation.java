package dataPrep;

import java.io.Serializable;
import java.util.Date;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.GreaterThan;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.types.DataTypes;

import util.MRMIndexes;
import exceptions.MRMNoFrameDataFoundException;

public class DataPreparation implements Serializable {

	private static final long serialVersionUID = -1153066584198837791L;

	// Initializing the logger.
	static final Logger mrmDevLogger = Logger.getLogger(DataPreparation.class);

	/**
	 * This method prepares the data.
	 * 
	 * @param inputData
	 * @param incentive
	 * @return dataOutLier
	 * @throws MRMNoFrameDataFoundException
	 */
	public DataFrame prepareData(DataFrame inputData, String incentive)
			throws MRMNoFrameDataFoundException {
		DataFrame dataOutLier;
		mrmDevLogger.warn("data preparation started at :::" + new Date());
		if (null == inputData) {
			throw new MRMNoFrameDataFoundException(
					MRMIndexes.getValue("NO_FRAME_DATA"));
		} else {
			// created a new column incentive type
			inputData = inputData.withColumn("incentive_type", new Column(
					new Literal(incentive, DataTypes.StringType)));

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed(incentive + "_daily_sales",
					incentive + "_daily_sales");

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed("daily_sales",
					"daily_sales");

			// Creating a new column low_sale_flag
			inputData = inputData.withColumn(
					"low_sale_flag",
					new Column(new If(new GreaterThan(inputData
							.resolve(incentive + "_daily_sales"), new Multiply(
							new Literal(0.05, DataTypes.DoubleType), inputData
									.resolve("daily_sales"))), new Literal(1,
							DataTypes.IntegerType), new Literal(0,
							DataTypes.IntegerType))));

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed(incentive + "_sales_share",
					incentive + "_sales_share");

			// creating a new column to use it for further operations
			inputData = inputData.withColumn("sales_share", new Column(
					inputData.resolve(incentive + "_sales_share")));

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed(incentive
					+ "_incentive_comp_ratio", incentive
					+ "_incentive_comp_ratio");

			// creating a new column to use it for further operations
			inputData = inputData.withColumn(
					"incentive_comp_ratio",
					new Column(inputData.resolve(incentive
							+ "_incentive_comp_ratio")));

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed(incentive
					+ "_msrp_comp_ratio", incentive + "_msrp_comp_ratio");

			// creating a new column to use it for further operations
			inputData = inputData.withColumn("msrp_comp_ratio", new Column(
					inputData.resolve(incentive + "_msrp_comp_ratio")));

			// creating a new column to use it for further operations
			inputData = inputData.withColumn(
					"incentive_pct_comp_ratio",
					new Column(new Divide(inputData
							.resolve("incentive_comp_ratio"), inputData
							.resolve("msrp_comp_ratio"))));

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed(incentive
					+ "_comp_incentive", incentive + "_comp_incentive");

			// creating a new column to use it for further operations
			inputData = inputData.withColumn("comp_incentive", new Column(
					inputData.resolve(incentive + "_comp_incentive")));

			// renaming the column name to use it for further operations
			inputData = inputData.withColumnRenamed("daily_sales",
					"daily_sales");

			// creating a new column to use it for further operations
			inputData = inputData.withColumn("overall_daily_sales", new Column(
					inputData.resolve("daily_sales")));

			// creating a new column to use it for further operations
			inputData = inputData.withColumn("daily_sales_new", new Column(
					inputData.resolve(incentive + "_daily_sales")));

			// creating a new column to use it for further operations
			inputData = inputData.withColumn("dependent_variable", new Column(
					inputData.resolve(incentive + "_sales_share")));

			// Filtering the data frame with low sales flag > 0
			dataOutLier = inputData.select("make_nm", "series_nm", "car_truck",
					"model_year", "region_code", "month", "business_month",
					"in_market_stage", "gas_price", "gas_price_chg_pct",
					"overall_daily_sales", "daily_sales_new",
					"contest_month_flg", "thrust_month_flg", "cftp",
					"life_cycle_position", "major_change", "minor_change",
					"comp_life_cycle_position", "comp_major_change",
					"comp_major_change_plus_1", "japan_tsunami",
					"incentive_type", "low_sale_flag", "sales_share",
					"incentive_comp_ratio", "msrp_comp_ratio",
					"incentive_pct_comp_ratio", "comp_incentive",
					"dependent_variable").where(
					new Column(new GreaterThan(inputData
							.resolve("low_sale_flag"), new Literal(0,
							DataTypes.IntegerType))));
			mrmDevLogger.warn("data preparation ended at :::" + new Date());
		}
		return dataOutLier;
	}
}
