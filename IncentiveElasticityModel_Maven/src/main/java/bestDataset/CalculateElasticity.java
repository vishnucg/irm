package bestDataset;

import static org.apache.spark.sql.functions.avg;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Divide;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.types.DataTypes;

import util.MRMIndexes;
import exceptions.MRMNoFrameDataFoundException;

public class CalculateElasticity implements Serializable {

	private static final long serialVersionUID = 534712989164281973L;

	// Initializing the logger.
	static final Logger mrmDevLogger = Logger
			.getLogger(CalculateElasticity.class);

	/**
	 * This method returns the data set based on the elasticity value.
	 * 
	 * @param inctvCoef
	 * @param dat
	 * @param series
	 * @param inctvType
	 * @param salesShareOutlier
	 * @param inctvRatioOutlier
	 * @param useInctvPctRation
	 * @param datasetId
	 * @return tmpElas
	 */
	public static DataFrame calcElasticity(double inctvCoef, DataFrame dat,
			String series, String inctvType, double salesShareOutlier,
			double inctvRatioOutlier, boolean useInctvPctRation, int datasetId)
			throws MRMNoFrameDataFoundException {
		DataFrame tmpElas;
		if (null == dat) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_FRAME_DATA"));
			throw new MRMNoFrameDataFoundException(
					MRMIndexes.getValue("NO_FRAME_DATA"));
		} else {
			// Calculating the temporary elasticity
			tmpElas = dat.groupBy("model_year", "region_code").agg(
					avg("comp_incentive").as("avg_comp_incentive"),
					avg("cftp").as("avg_cftp"),
					avg("pred_value").as("avg_pred_sales"),
					avg("msrp_comp_ratio").as("avg_comp_msrp_ratio"),
					functions.count("cftp").as("num_obs"),
					dat.col("region_code"), dat.col("model_year"));
			mrmDevLogger.info("TMPELAS PRINTING :::::");

			// All year agg.
			DataFrame allYearAgg = dat.groupBy("region_code").agg(
					avg("comp_incentive").as("avg_comp_incentive"),
					avg("cftp").as("avg_cftp"),
					avg("pred_value").as("avg_pred_sales"),
					avg("msrp_comp_ratio").as("avg_comp_msrp_ratio"),
					functions.count("cftp").as("num_obs"),
					dat.col("region_code"));
			mrmDevLogger.info("allYearAgg PRINTING :::::");

			allYearAgg = allYearAgg.withColumn("model_year", new Column(
					new Literal("ALL", DataTypes.StringType)));
			tmpElas = tmpElas.unionAll(allYearAgg);
			tmpElas = tmpElas.withColumnRenamed("region_code", "region_code");
			tmpElas = tmpElas
					.withColumn(
							"elasticity",
							new Column(new Multiply(new Literal(inctvCoef,
									DataTypes.DoubleType), new Divide(tmpElas
									.resolve("avg_cftp"), new Multiply(tmpElas
									.resolve("avg_pred_sales"), new Divide(
									tmpElas.resolve("avg_comp_incentive"),
									useInctvPctRation ? tmpElas
											.resolve("avg_comp_msrp_ratio")
											: new Literal(1D,
													DataTypes.DoubleType)))))));
			tmpElas = tmpElas.withColumnRenamed("model_year", "model_year");

			tmpElas = tmpElas.select(new Column(new Literal(series,
					DataTypes.IntegerType)).as("Series"), new Column(
					new Literal(inctvType, DataTypes.StringType))
					.as("Inventive_Type"), new Column(new Literal(
					salesShareOutlier, DataTypes.DoubleType))
					.as("Sales_Share_Outlier"), new Column(new Literal(
					inctvRatioOutlier, DataTypes.DoubleType))
					.as("Incentive_Ration_Outlier"), new Column(new Literal(
					inctvCoef >= 0 ? "Good" : "Bad", DataTypes.StringType))
					.as("Model_Performance"), tmpElas.col("elasticity"),
					tmpElas.col("region_code"), tmpElas.col("num_obs"),
					new Column(new Literal(datasetId, DataTypes.IntegerType))
							.as("dataset_id"), tmpElas.col("model_year"));
		}
		return tmpElas;
	}
}
