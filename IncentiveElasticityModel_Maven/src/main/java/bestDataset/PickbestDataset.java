package bestDataset;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import regression.IncentiveElasticityModelDTO;
import regression.LinearRegressionWrapper;
import scala.collection.JavaConversions;
import util.MRMIndexes;
import util.NetworkCommunicator;
import variableSelection.VariableSelection;
import exceptions.MRMBaseException;
import exceptions.MRMHeaderNotFoundException;
import exceptions.MRMLinearModelNotFoundException;

public class PickbestDataset implements Serializable {

	private static final long serialVersionUID = 3003300503802949980L;

	static final Logger mrmDevLogger = Logger.getLogger(PickbestDataset.class);

	/**
	 * This method needs to find out the best data set.
	 * 
	 * @param dataOutlierFrame
	 * @param series
	 * @param regionCode
	 * @param incentive
	 * @param parameter_map
	 * @return incentiveElasticityModelDTO
	 * @throws IOException
	 * @throws MRMBaseException
	 */

	public IncentiveElasticityModelDTO bestdataset(DataFrame dataOutlierFrame,
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws IOException, MRMBaseException {

		mrmDevLogger.warn("best data set started" + new Date());
		// Returning map with data frame.

		mrmDevLogger.warn("setDataFrameOurlier started at :::" + new Date());
		// Setting the data frame out lier.
		incentiveElasticityModelDTO.setDataFrameOurlier(dataOutlierFrame);
		mrmDevLogger.warn("setDataFrameOurlier ended at :::" + new Date());

		mrmDevLogger.warn("getRowsPercent started at :::" + new Date());

		// Setting rows percent.
		incentiveElasticityModelDTO = getRowsPercent(incentiveElasticityModelDTO);
		mrmDevLogger.warn("getRowsPercent ended at :::" + new Date());

		mrmDevLogger.warn("getPercentile2 started at :::" + new Date());

		// Setting percentile2 values.
		incentiveElasticityModelDTO = getPercentile2(incentiveElasticityModelDTO);
		mrmDevLogger.warn("getPercentile2 ended at :::" + new Date());

		mrmDevLogger.warn("selectExpression started at :::" + new Date());

		// Select expressions.
		incentiveElasticityModelDTO = selectExpression(incentiveElasticityModelDTO);
		mrmDevLogger.warn("selectExpression ended at :::" + new Date());

		mrmDevLogger.warn("findBestDataSet strated at :::" + new Date());

		// Finding the best dataset.
		incentiveElasticityModelDTO = findBestDataSet(incentiveElasticityModelDTO);

		mrmDevLogger.warn("findBestDataSet ended at :::" + new Date());

		mrmDevLogger.warn("setBestDataSetMap started at :::" + new Date());

		// Set best data set map.
		mrmDevLogger.warn("setBestDataSetMap ended at :::" + new Date());

		return incentiveElasticityModelDTO;
	}

	/**
	 * This method create a out lier values for the sales share.Finding the
	 * 1%,5%,10% values for sales share column.
	 * 
	 * @param incentiveElasticityModelDTO
	 */

	private IncentiveElasticityModelDTO getRowsPercent(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO) {
		// Creating a percentiles
		double[] doubles2 = incentiveElasticityModelDTO.getDoubles2();
		double[] percentile1 = new double[doubles2.length];

		// Sorting sales_share values to create a percentile outliers for that
		// column
		Row[] rowspercent = incentiveElasticityModelDTO.getDataFrameOurlier()
				.select("sales_share").sort("sales_share").collect();

		/*
		 * create a outlier values for the sales share.Finding the 1%,5%,10%
		 * values for sales share column.
		 */
		for (int i = 0; i < doubles2.length; i++) {
			double m = doubles2[i] * (rowspercent.length + 1);
			int elementPosition = (int) m - 1;
			mrmDevLogger.warn("TOTAL elementPosition TO position: 0 "
					+ elementPosition);
			if (elementPosition >= rowspercent.length - 1) {
				percentile1[i] = rowspercent[rowspercent.length - 1]
						.getDouble(0);
			} else {
				if (m < 1) {
					percentile1[i] = rowspercent[0].getDouble(0);
				} else {
					percentile1[i] = rowspercent[elementPosition].getDouble(0)
							+ (rowspercent[elementPosition + 1].getDouble(0) - rowspercent[elementPosition]
									.getDouble(0)) * (m % 1);
				}
			}
		}
		// Sets the rows percent and percentile1
		for (Double p2 : percentile1) {
			mrmDevLogger.warn("sales share percentiles::: " + p2);
		}
		incentiveElasticityModelDTO.setRowspercent(rowspercent);
		incentiveElasticityModelDTO.setPercentile1(percentile1);
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method creates a out lier values for the sales share.Finding the
	 * 1%,5%,10% values for incentive_comp_ratio column.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 */
	private IncentiveElasticityModelDTO getPercentile2(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO) {
		// Initializing the arrays.
		double[] doubles2 = incentiveElasticityModelDTO.getDoubles2();
		double[] percentile2 = new double[doubles2.length];

		// Get rows out lier.
		Row[] rowspercent = incentiveElasticityModelDTO.getDataFrameOurlier()
				.select("incentive_comp_ratio").sort("incentive_comp_ratio")
				.collect();

		for (int i = 0; i < doubles2.length; i++) {
			double m = doubles2[i] * (rowspercent.length + 1);
			int elementPosition = (int) m - 1;
			if (elementPosition >= rowspercent.length - 1) {
				percentile2[i] = rowspercent[rowspercent.length - 1]
						.getDouble(0);
			} else {
				if (m < 1) {
					percentile2[i] = rowspercent[0].getDouble(0);
				} else {
					percentile2[i] = rowspercent[elementPosition].getDouble(0)
							+ (rowspercent[elementPosition + 1].getDouble(0) - rowspercent[elementPosition]
									.getDouble(0)) * (m % 1);
				}
			}
		}
		for (Double p2 : percentile2) {
			mrmDevLogger.warn("incentive percentiles::: " + p2);
		}
		incentiveElasticityModelDTO.setPercentile2(percentile2);
		return incentiveElasticityModelDTO;
	}

	/**
	 * 
	 * Here we are looking for the outlier values. Marking the sales share,
	 * incentive comp ratio outlier's with appropriate outlier values(Replacing
	 * 100's with 1's or 5's or 10's).
	 * 
	 * @param incentiveElasticityModelDTO
	 */
	private IncentiveElasticityModelDTO selectExpression(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO) {
		DataFrame dataOutlier = incentiveElasticityModelDTO
				.getDataFrameOurlier();
		double[] doubles = incentiveElasticityModelDTO.getDoubles();
		double[] percentile1 = incentiveElasticityModelDTO.getPercentile1();
		double[] doubles2 = incentiveElasticityModelDTO.getDoubles2();
		double[] percentile2 = incentiveElasticityModelDTO.getPercentile2();

		// Create a new column sales share outlier and add value "100" in each
		// row of the data frame for this column.
		dataOutlier = dataOutlier.withColumn("sales_share_outlier", new Column(
				new Literal(100, DataTypes.IntegerType)));

		// Create a new column incentive_ratio_outlier and add value "100" in
		// each row of the data frame for this column.
		dataOutlier = dataOutlier.withColumn("incentive_ratio_outlier",
				new Column(new Literal(100, DataTypes.IntegerType)));

		// Printing the sales share percentile values for validation purpose
		for (int i = 0; i < 6; i++) {
			mrmDevLogger.warn(String.format("%.12f",
					incentiveElasticityModelDTO.getPercentile1()[i]));
		}

		/*
		 * Here we are looking for the outlier values. Marking the sales share,
		 * incentive comp ratio outlier's with appropriate outlier
		 * values(Replacing 100's with 1's or 5's or 10's).
		 */
		for (int i = 0; i < doubles.length; i++) {
			dataOutlier = dataOutlier
					.selectExpr(
							"case when sales_share_outlier = 100 and ((sales_share < "
									+ String.format("%.12f", percentile1[i])
									+ ") or ("
									+ "sales_share > "
									+ String.format(
											"%.12f",
											percentile1[(doubles2.length - i - 1)])
									+ ")) then  "
									+ (doubles[i] * 100)
									+ " else sales_share_outlier end as sales_share_outlier",
							"case when incentive_ratio_outlier = 100 and ((incentive_comp_ratio < "
									+ String.format("%.12f", percentile2[i])
									+ ") or ("
									+ "incentive_comp_ratio > "
									+ String.format(
											"%.12f",
											percentile2[doubles2.length - i - 1])
									+ ")) then "
									+ (doubles[i] * 100)
									+ " else incentive_ratio_outlier end as incentive_ratio_outlier ",
							"make_nm", "series_nm", "car_truck", "model_year",
							"region_code", "month", "business_month",
							"in_market_stage", "gas_price",
							"gas_price_chg_pct", "overall_daily_sales",
							"daily_sales_new", "contest_month_flg",
							"thrust_month_flg", "cftp", "life_cycle_position",
							"major_change", "minor_change",
							"comp_life_cycle_position", "comp_major_change",
							"comp_major_change_plus_1", "japan_tsunami",
							"incentive_type", "low_sale_flag", "sales_share",
							"incentive_comp_ratio", "msrp_comp_ratio",
							"incentive_pct_comp_ratio", "comp_incentive",
							"dependent_variable");

		}
		incentiveElasticityModelDTO.setDataFrameOurlier(dataOutlier);
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method lopping through the data frame and filtering the outliers at
	 * each and every level and get the best dataset from the dataframe with out
	 * outliers
	 * 
	 * 
	 * @param incentiveElasticityModelDTO
	 * @throws MRMBaseException
	 */
	private IncentiveElasticityModelDTO findBestDataSet(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws MRMBaseException {
		// Reading the dataframe outlier, share incentive excel values and
		// incentive ratio excel values.
		DataFrame dataOutlierDataFrame = incentiveElasticityModelDTO
				.getDataFrameOurlier();
		double[] shareInctvExcl = incentiveElasticityModelDTO
				.getShareInctvExcl();
		double[] inctvRatioExcl = incentiveElasticityModelDTO
				.getInctvRatioExcl();

		// initializing the variables to find the best data set
		double bestelasticity = 0.0;
		String indepstring = "";
		/*
		 * lopping through the data frame and filtering the outliers at each and
		 * every level and get the best dataset from the dataframe with out
		 * outliers
		 */
		for (int i = 0; i < shareInctvExcl.length; i++) {
			DataFrame dat = dataOutlierDataFrame
					.filter("(sales_share_outlier > " + shareInctvExcl[i]
							+ ") AND (incentive_ratio_outlier > "
							+ inctvRatioExcl[i] + ")");
			mrmDevLogger.warn("before variable selection ::"
					+ incentiveElasticityModelDTO.getIndependentList().size());

			if (null == dat
					|| dat.count() > incentiveElasticityModelDTO
							.getIndependentList().size()) {
				continue;
			} else {
				mrmDevLogger.warn("Number of lines are ****** " + dat.count());
				// Reading the data set.
				incentiveElasticityModelDTO = readDataset(
						incentiveElasticityModelDTO, dat);
				try {
					// Calling variable selection.
					incentiveElasticityModelDTO = getVariables(incentiveElasticityModelDTO);
				} catch (Exception e) {
					mrmDevLogger.warn("variable selection failed:: "
							+ e.getMessage());
					continue;
				}
				// Checking the variables available or not.
				if (incentiveElasticityModelDTO.isContainsList()
						&& incentiveElasticityModelDTO.getIndependentList()
								.size() > 1) {
					// Getting the residuals.
					try {
						incentiveElasticityModelDTO = getResiduals(
								incentiveElasticityModelDTO, dat);
					} catch (Exception e) {
						mrmDevLogger.warn("Regression failed :::"
								+ e.getMessage());
						continue;
					}
					// Getting P-Values.
					if (null == incentiveElasticityModelDTO.getResiduals()) {
						mrmDevLogger.warn("Beta null");
						continue;
					} else {
						incentiveElasticityModelDTO = getPvalue(
								incentiveElasticityModelDTO, dat);

						// Getting the pred values.
						incentiveElasticityModelDTO = getPredValue(incentiveElasticityModelDTO);

						// Calculate elasticity by passing all enough parameters
						// to
						// the
						// calElasticty function.
						DataFrame incentiveResult = CalculateElasticity
								.calcElasticity(
										incentiveElasticityModelDTO
												.getInctvCoef(),
										incentiveElasticityModelDTO.getDat(),
										incentiveElasticityModelDTO.getSeries(),
										incentiveElasticityModelDTO
												.getIncentive(),
										shareInctvExcl[i], inctvRatioExcl[i],
										false, i + 1);
						if (null != incentiveResult) {
							// Calculating the average elasticity
							Double avgElasticity = 0.0;
							for (Row avgelas : incentiveResult.collect()) {
								if (avgelas.getString(9).equals("ALL")) {
									avgElasticity = avgElasticity
											+ avgelas.getDouble(5);
								}
							}
							mrmDevLogger.warn("Avg elasticity ::::: "
									+ avgElasticity);
							// Picking the best dataset based on best pvalue and
							// best
							// elasticity
							if (incentiveElasticityModelDTO.getpValue() < 0.3
									&& avgElasticity > bestelasticity) {
								incentiveElasticityModelDTO
										.setBestDataSet(incentiveElasticityModelDTO
												.getInctvbayesian());
								incentiveElasticityModelDTO
										.setBestpvalue(incentiveElasticityModelDTO
												.getpValue());
								incentiveElasticityModelDTO
										.setIndependentList(incentiveElasticityModelDTO
												.getIndependentList());
							}
						}
					}
				}
			}
		}
		if (null == incentiveElasticityModelDTO.getIndependentList()
				|| incentiveElasticityModelDTO.getIndependentList().isEmpty()) {
			mrmDevLogger
					.warn("independent list is empty at the end of pick dataset ::::");
		} else {
			for (String indep : incentiveElasticityModelDTO
					.getIndependentList()) {
				indepstring += indep + ",";
			}
			if ("" == indepstring) {
				mrmDevLogger.warn("indep string :::" + indepstring);
			} else {
				incentiveElasticityModelDTO.setIndependentString(indepstring
						.substring(0, indepstring.length() - 1));
				incentiveElasticityModelDTO
						.setBestDataSet(incentiveElasticityModelDTO
								.getBestDataSet().withColumnRenamed("fitted1",
										"pred_value"));
			}
		}
		return incentiveElasticityModelDTO;
	}

	/**
	 * 
	 * @param incentiveElasticityModelDTO
	 * @param dat
	 * @return incentiveElasticityModelDTO
	 * @throws MRMHeaderNotFoundException
	 */
	private IncentiveElasticityModelDTO readDataset(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO,
			DataFrame dat) throws MRMHeaderNotFoundException {
		mrmDevLogger.warn("Number of lines are ****** " + dat.count());
		String finalString = "";

		// Loading the actual data and creating the RDD.
		String[] datacol = dat.columns();
		for (int d = 0; d < datacol.length; d++) {
			if (!finalString.equals("")) {
				finalString = finalString + ",";
			}
			finalString = finalString + datacol[d];
		}
		mrmDevLogger.warn(finalString);
		JavaRDD<String> colheader;
		colheader = NetworkCommunicator.getJavaSparkContext().parallelize(
				Arrays.asList(finalString));

		// Check the header data.
		if (null == colheader || colheader.isEmpty()) {
			throw new MRMHeaderNotFoundException(
					MRMIndexes.getValue("COLUMN_HEADER_WARN"));
		} else {
			JavaRDD<String> data = dat.toJavaRDD().map(
					new Function<Row, String>() {
						private static final long serialVersionUID = 2453646105748597983L;

						@Override
						public String call(Row row) throws Exception {
							return row.mkString(",");
						}
					});
			colheader = colheader.union(data);
		}
		incentiveElasticityModelDTO.setVarSelectionInput(colheader);
		return incentiveElasticityModelDTO;
	}

	/**
	 * This module gives the variables back after variable selection.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @return incentiveElasticityModelDTO
	 * @throws MRMBaseException
	 */
	private IncentiveElasticityModelDTO getVariables(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO)
			throws MRMBaseException {
		mrmDevLogger.warn(" ******** entering into variable selection ****** ");
		VariableSelection variableSelection = new VariableSelection();
		Map<String, Double> variablemap;
		List<String> independentList = new ArrayList<>();

		// Calling the variable selection module.
		variablemap = variableSelection.getVariables(
				incentiveElasticityModelDTO.getVarSelectionInput(),
				incentiveElasticityModelDTO.getParameterMap(),
				NetworkCommunicator.getJavaSparkContext(), 10);
		incentiveElasticityModelDTO.setVariablemap(variablemap);
		if (null == variablemap || variablemap.isEmpty()) {
			mrmDevLogger.warn("independent list is empty");
		} else {
			// Get independent list.
			if (null == variablemap.keySet() || variablemap.keySet().isEmpty()) {
				mrmDevLogger.warn("variable selection keyset is empty ");
			} else {
				independentList.addAll(variablemap.keySet());
				incentiveElasticityModelDTO.setIndependentList(independentList);
				if (independentList.isEmpty()) {
					incentiveElasticityModelDTO.setContainsList(false);
				} else {
					incentiveElasticityModelDTO.setContainsList(true);
					if (independentList.contains("incentive_comp_ratio")) {
						independentList.remove(independentList
								.indexOf("incentive_comp_ratio"));
					}
				}
			}
		}
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method gives the residuals.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @param dat
	 * @return incentiveElasticityModelDTO
	 * @throws MRMLinearModelNotFoundException
	 */
	private IncentiveElasticityModelDTO getResiduals(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO,
			DataFrame dat) throws MRMLinearModelNotFoundException {
		// initialized the beta to null because we are generating the double
		// array inside the try block
		double[] beta = null;

		// Calling the linear regression function to find the residuals for
		// the dataset
		mrmDevLogger.warn(" independent list is "
				+ incentiveElasticityModelDTO.getIndependentList());
		try {
			LinearRegressionWrapper regressionWrapper = new LinearRegressionWrapper(
					"dependent_variable",
					incentiveElasticityModelDTO.getIndependentList());
			// incentiveElasticityModelDTO.getIndependentList());
			regressionWrapper.newSampleData(dat);

			// calculate Residual
			beta = regressionWrapper.estimateResiduals();
		} catch (Exception e) {
			mrmDevLogger
					.warn("*********** Regression failed ********** series: "
							+ incentiveElasticityModelDTO.getSeries()
							+ "region: "
							+ incentiveElasticityModelDTO.getRegion()
							+ "incentive : "
							+ incentiveElasticityModelDTO.getIncentive()
							+ e.getMessage());

		}
		incentiveElasticityModelDTO.setResiduals(beta);
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method Calling regression wrapper class to perform regression for
	 * residuals we calculated as an independent variable and incentive comp
	 * ratio as dependent variable.Program automatically use the simple
	 * regression if we have one indepedent variable.
	 * 
	 * @param incentiveElasticityModelDTO
	 * @param dat
	 * @return incentiveElasticityModelDTO
	 * @throws MRMLinearModelNotFoundException
	 */
	private IncentiveElasticityModelDTO getPvalue(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO,
			DataFrame dat) throws MRMLinearModelNotFoundException {
		// Collect data from the data frame.
		List<Row> rows = dat.collectAsList();
		List<Row> rowsWithResidual = new ArrayList<>(rows.size());

		for (int j = 0; j < rows.size(); j++) {
			List<Object> newRow = new ArrayList<>(
					JavaConversions.asJavaList(rows.get(j).toSeq()));
			try {
				newRow.add(incentiveElasticityModelDTO.getResiduals()[j]);
			} catch (Exception e) {
				mrmDevLogger.warn("regression failed ::::" + e.getMessage());
			}
			rowsWithResidual.add(RowFactory.create(newRow
					.toArray(new Object[newRow.size()])));
		}

		// Get spark context
		try {
			JavaRDD<Row> rowJavaRDD = NetworkCommunicator.getJavaSparkContext()
					.parallelize(rowsWithResidual);
			List<StructField> structFields = new ArrayList<>(
					JavaConversions.asJavaList(dat.schema()));
			structFields.add(new StructField("residual", DataTypes.DoubleType,
					true, Metadata.empty()));
			StructType structType = new StructType(
					structFields.toArray(new StructField[structFields.size()]));
			dat = incentiveElasticityModelDTO.getHiveContext().createDataFrame(
					rowJavaRDD, structType);

			// Finding the fitted values for the dataset. Fitted values =
			// dependent - residual
			dat = dat.withColumn("fitted1",
					new Column(new Subtract(dat.resolve("dependent_variable"),
							dat.resolve("residual"))));

			incentiveElasticityModelDTO.setInctvbayesian(dat);
		} catch (Exception e) {
			mrmDevLogger.warn("regression failed ::::" + e.getMessage());
		}
		/*
		 * Calling regression wrapper class to perform regression for residuals
		 * we calculated as an independent variable and incentive comp ratio as
		 * dependent variable.Program automatically use the simple regression if
		 * we have one indepedent variable.
		 */
		try {
			LinearRegressionWrapper simpleregression = new LinearRegressionWrapper(
					"incentive_comp_ratio", Arrays.asList("residual"));

			// Adding data to the regression
			simpleregression.newSampleData(dat);

			// Finding the slope and pvalue. These values will be useful to
			// analyse the best dat set.
			Double inctvCoef = simpleregression.getSlope();
			Double pvalue = simpleregression.getpvalue();
			mrmDevLogger.warn("COEFFICIENT AND PVALUE: " + inctvCoef
					+ ":::::::" + pvalue);
			incentiveElasticityModelDTO.setpValue(pvalue);
			incentiveElasticityModelDTO.setDat(dat);
			incentiveElasticityModelDTO.setInctvCoef(inctvCoef);
			incentiveElasticityModelDTO.setSimpleregression(simpleregression);
		} catch (Exception exception) {
			mrmDevLogger.warn("Regression failed::::" + exception.getMessage());

		}
		return incentiveElasticityModelDTO;
	}

	/**
	 * This method returns the pred value back
	 * 
	 * @param incentiveElasticityModelDTO
	 */
	private IncentiveElasticityModelDTO getPredValue(
			IncentiveElasticityModelDTO incentiveElasticityModelDTO) {
		DataFrame dat = incentiveElasticityModelDTO.getDat();
		LinearRegressionWrapper simpleregression = incentiveElasticityModelDTO
				.getSimpleregression();
		List<Row> rows1 = dat.collectAsList();
		List<Row> rowsWithResidual1 = new ArrayList<>(rows1.size());

		// loop the rows.
		for (int j = 0; j < rows1.size(); j++) {
			List<Object> newRow = new ArrayList<>(
					JavaConversions.asJavaList(rows1.get(j).toSeq()));
			newRow.add(simpleregression.predict(rows1.get(j).getDouble(27)));
			rowsWithResidual1.add(RowFactory.create(newRow
					.toArray(new Object[newRow.size()])));
		}
		JavaRDD<Row> rowJavaRDD1 = NetworkCommunicator.getJavaSparkContext()
				.parallelize(rowsWithResidual1);
		List<StructField> structFields1 = new ArrayList<>(
				JavaConversions.asJavaList(dat.schema()));
		structFields1.add(new StructField("fitted2", DataTypes.DoubleType,
				true, Metadata.empty()));
		StructType structType1 = new StructType(
				structFields1.toArray(new StructField[structFields1.size()])); // INCENTIVE_RATIO
																				// //
																				// PREDICT
		// COEFF*INDE+SLOPE
		dat = incentiveElasticityModelDTO.getHiveContext().createDataFrame(
				rowJavaRDD1, structType1);

		// pred_value is calculated by adding fitted values from both
		// regressions.
		dat = dat.withColumn(
				"pred_value",
				new Column(new Add(dat.resolve("fitted1"), dat
						.resolve("fitted2"))));
		incentiveElasticityModelDTO.setDat(dat);
		return incentiveElasticityModelDTO;
	}

}
