package com.toyota.analytix.mrm.msrp.bestDataset;

import static org.apache.spark.sql.functions.avg;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.types.DataTypes;

public class CalculateElasticity {
	private static final Logger logger = Logger.getLogger(CalculateElasticity.class);
	public static DataFrame calcElasticity(double msrpCoef, DataFrame dat,
			String series, double salesShareOutlier,
			double msrpRatioOutlier, double seriesMSRPCFTPcoef, int datasetId) {
        
		//Region code and model year grouping
		DataFrame tmpElas = dat.groupBy("model_year", "region_code").agg(
				avg("comp_msrp").as("avg_comp_msrp"),
				avg("cftp").as("avg_cftp"),
				avg("pred_value").as("avg_pred_sales"),
				functions.count("cftp").as("num_obs"),
				dat.col("region_code"), dat.col("model_year"));
		logger.debug("TMPELAS PRINTING :::::");
		tmpElas.show();
		
		//Aggregate across years 
		DataFrame allYearAgg = dat.groupBy("region_code").agg(
				avg("comp_msrp").as("avg_comp_msrp"),
				avg("cftp").as("avg_cftp"),
				avg("pred_value").as("avg_pred_sales"),
				functions.count("cftp").as("num_obs"), dat.col("region_code"));
		logger.debug("allYearAgg PRINTING :::::");
		allYearAgg.show();
		
		// Add Model_Year column with default value as ALL
		allYearAgg = allYearAgg.withColumn("model_year", new Column(
				new Literal("ALL", DataTypes.StringType)));

		//union of yearly aggregates and all year aggregates for regions
		tmpElas = tmpElas.unionAll(allYearAgg);
		
	
		//tmp.elas$Elasticity <- with(tmp.elas, MSRP.coef * (avg_cftp / (avg_pred_sales * MSRP.coef.in.CFTP * avg_comp_msrp)))
		tmpElas = tmpElas.withColumn(
				"elasticity",
				new Column(new Multiply(new Literal(msrpCoef,
						DataTypes.DoubleType), new Divide(tmpElas
						.resolve("avg_cftp"), new Multiply(tmpElas
						.resolve("avg_pred_sales"), new Multiply(tmpElas
						.resolve("avg_comp_msrp"),new Literal(
								seriesMSRPCFTPcoef, DataTypes.DoubleType)))))));

		logger.debug("TMPELAS PRINTING :::::");
		tmpElas.show();

		//To fix the hive issue, we need to rename columns
		tmpElas = tmpElas.withColumnRenamed("model_year", "model_year");
        
		DataFrame elasticity = tmpElas.select(
				// col0 - series
				new Column(new Literal(series, DataTypes.IntegerType)).as("Series"),
				// col1 - model_year
				tmpElas.col("model_year").as("Model_Year"),
				// col2 - sales_share_outlier
				new Column(new Literal(salesShareOutlier, DataTypes.DoubleType)).as("Sales_Share_Outlier"),
				// col3 - MSRP_Ratio_Outlier
				new Column(new Literal(msrpRatioOutlier, DataTypes.DoubleType)).as("MSRP_Ratio_Outlier"),
				// col4 - Model_Performance
				new Column(new Literal(msrpCoef <= 0 ? "Good" : "Bad", DataTypes.StringType)).as("Model_Performance"),
				// col5 - region_code
				tmpElas.col("region_code"),
				// col6 - elasticity
				tmpElas.col("elasticity"),
				// col7 - num_obs
				tmpElas.col("num_obs"),
				// col8- dataset_id
				new Column(new Literal(datasetId, DataTypes.IntegerType)).as("dataset_id"));

		   return elasticity;
	}
}
