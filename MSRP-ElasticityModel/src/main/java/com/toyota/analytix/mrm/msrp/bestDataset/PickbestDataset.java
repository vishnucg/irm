package com.toyota.analytix.mrm.msrp.bestDataset;

import static com.toyota.analytix.common.spark.SparkRunnableModel.hiveContext;
import static com.toyota.analytix.common.spark.SparkRunnableModel.sc;
import static java.lang.Math.abs;
import static java.lang.Math.min;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.common.regression.LinearRegressionWrapper;
import com.toyota.analytix.common.util.MRMUtil;
import com.toyota.analytix.common.util.QueryProperties;
import com.toyota.analytix.common.util.SimpleTimer;
import com.toyota.analytix.common.variableSelection.VariableSelection;
import com.toyota.analytix.commons.spark.DataFrameUtil;
import com.toyota.analytix.mrm.msrp.dataPrep.ModelParameters;

import scala.collection.JavaConversions;

public class PickbestDataset implements Serializable {

	private final static Logger logger = Logger.getLogger (PickbestDataset.class);

	private static final long serialVersionUID = 3003300503802949980L;
	
	double shareOutlier = 0;
	double msrpOutlier = 0;

	public PickbestResult bestDataSet(DataFrame dat_b4_outlier, String seriesName, ModelParameters modelParameters) {

		logger.info("***********In PickbestDataset:bestDataSet ********* for dataset size: "+ dat_b4_outlier.count());
		SimpleTimer pickBestTimer = new SimpleTimer();
		// Map<DataFrame, String> return_map = new HashMap<DataFrame, String>();

		// Calculate Percentiles for SALES_SHARE
		double[] salesSharePercentile = MRMUtil.getPercentiles(dat_b4_outlier, "SALES_SHARE",
				QueryProperties.getPercentileBuckets());

		// Calculate Percentiles for MSRP_COMP_RATIO
		double[] msrpCompRatioPercentile = MRMUtil.getPercentiles(dat_b4_outlier, "MSRP_COMP_RATIO",
				QueryProperties.getPercentileBuckets());

		// Update SALES_SHARE_OUTLIER and MSRP_RATIO_OUTLIER with scores of
		// 5,10,15 or hundred based on their percentile
		dat_b4_outlier = MRMUtil.applyPercentileScoresToColumns(dat_b4_outlier, QueryProperties.getPercentileBuckets(),
				salesSharePercentile, msrpCompRatioPercentile, "sales_share_outlier", "msrp_ratio_outlier");

		logger.info("***********Updated SALES_SHARE_OUTLIER, MSRP_RATIO_OUTLIER*********");
		dat_b4_outlier.show();

		// get the sales share exclusion score sets
		double[] salesShareExlusions = QueryProperties.getSalesShareExclusions();

		// get the MSRP exclusion score sets
		double[] MSRPRatioExclusions = QueryProperties.getMSRPRatioExclusions();

		// initializing the variables to find the best data set
		double bestelasticity = 0.0;
		double bestpvalue = 0.0;
		DataFrame bestdat = null;
		DataFrame dat = null;
		Double minMSRPElas = modelParameters.getMinMSRPElasticity(); 
		Double maxMSRPElas = modelParameters.getMaxMSRPElasticity(); 

		/*
		 * lopping through the data frame and filtering the outliers at each and
		 * every level and get the best dataset from the dataframe with out
		 * outliers
		 */
		
		//keep track of variable selection time
		long variableSelectionTimeMillis = 0L, elasticityTimeMillis = 0L, dataFrameToRDDTime= 0L, regressionTimeMillis= 0L;
		String finalFormula = "";
		logger.info("*********** Starting the for loop for best outlier set *********");
		for (int i = 0; i < salesShareExlusions.length; i++) {
			logger.info("**** Exclusion Set for i:" + i + " sales_share_outlier:" + salesShareExlusions[i]
					+ " msrp_ratio_outlier:" + MSRPRatioExclusions[i]+ "***");
			
			String filterString = "(sales_share_outlier > " + salesShareExlusions[i]
					+ ") AND (msrp_ratio_outlier > " + MSRPRatioExclusions[i] + ")";
			dat = dat_b4_outlier.filter(filterString);

			dat.show();
			long dataSize = dat.count();
			logger.info("Number of lines in the data after applying filter:" + filterString +":"+ dataSize);

			
			/*
			 * once we have filtered the outliers variable selection function
			 * will pick the best independent variable list from the list of
			 * columns.
			 */
			SimpleTimer t = new SimpleTimer();
			// Implemented variable selection

			// conversion of names, seperator, rdd
			// variable selection
			String finalString = "";
			// Loading the actual data and creating the RDD.
			String[] datacol = dat.columns();
			for (int d = 0; d < datacol.length; d++) {
				if (!finalString.equals("")) {
					finalString = finalString + ",";
				}
				finalString = finalString + datacol[d];
			}
			logger.info("Columns in the next line:\n"+finalString);
			JavaRDD<String> colheader = null;
			if (null != sc) {
				colheader = sc.parallelize(Arrays.asList(finalString));
			}

			JavaRDD<String> data = dat.toJavaRDD().map(new Function<Row, String>() {
				private static final long serialVersionUID = 2453646105748597983L;

				public String call(Row row) throws Exception {
					String rowStr = row.mkString(",");
					logger.debug(rowStr);
					return rowStr;
				}
			});
			
			colheader = colheader.union(data);
			for (String dta : colheader.collect()) {
				logger.info(dta);
			}
			long tEnd = t.endTimer();
			dataFrameToRDDTime += tEnd;
			logger.info("data size?? " + colheader.count()+ ". Time taken to convert Dataframe to RDD:" + tEnd + " millis");

			// Preparing the properties required to Variable
			// Selection.

			// Calling the method which will calculate the R square
			// value and VIF
			// Value based on input RDD.
			
			SimpleTimer vst = new SimpleTimer();
			List<String> indep_list = getEffectiveVariables(colheader, modelParameters);
			
			// hardcode variables
			// String[] str = new String[] { "model_year", "japan_tsunami",
			// "comp_major_change" };
			// String[] str = new String[] { "comp_major_change" };
			//			indep_list = Arrays.asList(str);
			
			String formula = Arrays.toString(indep_list.toArray());
			logger.info(" *** Variable Selection time :"+ vst.endTimer() + " millis. Variables:" + formula);
			variableSelectionTimeMillis += vst.endTimer();
			
			SimpleTimer rt = new SimpleTimer();
			// changing data for regression
			// initialized the beta to null because we are generating the double
			// array inside the try block
			double[] beta = null;

			logger.info("** Starting regression with independent variable list: "+ indep_list);

			if (indep_list == null || indep_list.size()==0){
				logger.warn( "Skipping Regression for "
						+ " Filter: " + filterString
						+ " series: " + seriesName
						+ " Data Size: " + dataSize
						+ ". Reason: No independent variable list. Skipping regression for this data filter. ");
				continue;
			}
			
			// Calling the linear regression function to find the residuals for
			// the dataset
			try {
				if (indep_list.size() == 1) {
					LinearRegressionWrapper simpleregression = new LinearRegressionWrapper("dependent_variable",
							indep_list);
					// Adding data to the regression
					simpleregression.newSampleData(dat);
					
					beta = new double[(int)dat.count()];
					double[] indepvar = new double[beta.length];
					double[] depvar = new double[beta.length];
					
					DataFrame indepDf = dat.select("dependent_variable",indep_list.get(0));
					logger.debug("indepDF");
					indepDf.show();					
					
					List<Row> rows = indepDf.collectAsList();
					for (int j = 0; j < rows.size(); j++) {
						depvar[j] = getDouble(rows.get(j).get(0));
						indepvar[j] = getDouble(rows.get(j).get(1));
						beta[j] = depvar[j] - simpleregression.predict(indepvar[j]);
					}
					//debug info
					//DEPENDENT = fitted(i.e intercept + coeff*independent var) + residuals.
					double intercept = simpleregression.getIntercept();
					double coeff = simpleregression.getSlope();
					logger.debug("dependent variable :\n" + Arrays.toString(depvar));
					logger.debug("independent variable ("+ indep_list.get(0) + "):\n" + Arrays.toString(indepvar));
					logger.debug("beta arr for simple regression:\n" + Arrays.toString(beta));
					logger.debug("intercept: " +intercept  + " coeff:"  + coeff);
					double[] sumVal = new double[beta.length];
					for (int j=0; j< sumVal.length; j++){
						sumVal[j] = intercept + coeff*indepvar[j] + beta[j];
					}
					logger.debug("intercept + coeff* indep + residual: " +Arrays.toString(sumVal) );
					
				} else {
					LinearRegressionWrapper regressionWrapper = new LinearRegressionWrapper("dependent_variable",
							indep_list);

					if (null != regressionWrapper) {
						regressionWrapper.newSampleData(dat);

						// calculate Residual
						beta = regressionWrapper.estimateResiduals();
						logger.info("number of residuals are ******* " + Arrays.toString(beta));
						List<Double> l = Arrays.asList(ArrayUtils.toObject(beta));
						logger.info("residual min: " + Collections.min(l) + " residual max:" + Collections.max(l));
						logger.info("Coefficients: " + Arrays.toString(regressionWrapper.getCoefficients().toArray()));
					} 
				}
			} catch (Exception e) {
				logger.warn("Skipping Regression for " + " Filter: " + filterString + " series: " + seriesName
						+ " Data Size: " + dataSize + ". Reason: " + e.getMessage()
						+ ". Skipping regression for this data filter. ", e);
				continue;
			}

			logger.info("** End regression with independent variable list: "+ indep_list);

			/*
			 * Here we are collecting the array of residual values we get
			 * through the regression and adding them to the dataset.
			 */
			List<Row> rows = dat.collectAsList();
			List<Row> rowsWithResidual = new ArrayList<Row>(rows.size());
			for (int j = 0; j < rows.size(); j++) {
				List<Object> newRow = new ArrayList<Object>(JavaConversions.asJavaList(rows.get(j).toSeq()));
				newRow.add(beta[j]);
				rowsWithResidual.add(RowFactory.create(newRow.toArray(new Object[newRow.size()])));
			}
			JavaRDD<Row> rowJavaRDD = sc.parallelize(rowsWithResidual);
			List<StructField> structFields = new ArrayList<StructField>(JavaConversions.asJavaList(dat.schema()));
			structFields.add(new StructField("residual", DataTypes.DoubleType, true, Metadata.empty()));
			StructType structType = new StructType(structFields.toArray(new StructField[structFields.size()]));
			dat = hiveContext.createDataFrame(rowJavaRDD, structType);

			logger.info(" Created Dataframe from RDD ");

			// Finding the fitted values for the dataset. Fitted values =
			// dependent - residual
			dat = dat.withColumn("fitted1",
					new Column(new Subtract(dat.resolve("dependent_variable"), dat.resolve("residual"))));

			/*
			 * Calling regression wrapper class to perform regression for
			 * residuals we calculated as an independent variable and incentive
			 * comp ratio as dependent variable.Program automatically use the
			 * simple regression if we have one indepedent variable.
			 */
			logger.info("** Starting regression with independent variable list: residual");
		
			LinearRegressionWrapper simpleregression = new LinearRegressionWrapper("msrp_comp_ratio",
					Arrays.asList("residual"));

			// Adding data to the regression
			simpleregression.newSampleData(dat);

			logger.info("** End regression with independent variable list: residual");
			
			// Finding the slope and pvalue. These values will be useful to
			// analyze the best dat set.
			Double regressionCoef = simpleregression.getSlope();
			// SimpleRegression simpleRegression = new SimpleRegression();
			Double pvalue = simpleregression.getpvalue();
			//logger.info("COEFFICIENT AND PVALUE: " + regressionCoef + ":::::::" + pvalue);
			
			/*
			 * Finding fitted2 value for the simple regression. Loop through the
			 * data frame and used predict(independent variable) and filled
			 * fitted2 values in each row.
			 */
			logger.info("******Data before fitted2********");
			dat.show();
			
			List<Row> rows1 = dat.collectAsList();
			List<Row> rowsWithResidual1 = new ArrayList<Row>(rows1.size());
			for (int j = 0; j < rows1.size(); j++) {
				List<Object> newRow = new ArrayList<Object>(JavaConversions.asJavaList(rows1.get(j).toSeq()));
				//Fit2 is the fit for the regression between MSRP and Residual. 
				newRow.add(simpleregression.predict(DataFrameUtil.<Double>getValueOfColumn(dat.columns(), rows1.get(j), "msrp_comp_ratio")));
				rowsWithResidual1.add(RowFactory.create(newRow.toArray(new Object[newRow.size()])));
			}
			JavaRDD<Row> rowJavaRDD1 = sc.parallelize(rowsWithResidual1);
			List<StructField> structFields1 = new ArrayList<StructField>(JavaConversions.asJavaList(dat.schema()));
			structFields1.add(new StructField("fitted2", DataTypes.DoubleType, true, Metadata.empty()));
			StructType structType1 = new StructType(structFields1.toArray(new StructField[structFields1.size()])); 
			
			dat = hiveContext.createDataFrame(rowJavaRDD1, structType1);
//			logger.info("******Data after fitted2********");
//			dat.show();

			// pred_value is calculated by adding fitted values from both
			// regressions.
			dat = dat.withColumn("pred_value", new Column(new Add(dat.resolve("fitted1"), dat.resolve("fitted2"))));
			logger.info("******Data after pred_value********");
			dat.show(31);
			logger.info(" ************ Regression complete. Time Taken:" + rt.endTimer() + " millis");
			regressionTimeMillis += rt.endTimer();

		    
			SimpleTimer elt = new SimpleTimer();
			//TODO do we need to get series is toyota or Lexus?
			//currenlty passing toyotaMSRPCoefInCFTP
			DataFrame elasticity_result = CalculateElasticity.calcElasticity(regressionCoef, dat, seriesName, 
					salesShareExlusions[i], MSRPRatioExclusions[i], getMSRPCoefInCFTP(seriesName, modelParameters), i + 1);
			
			logger.info("**** Elasticity Result *******");
			elasticity_result.show();
			
			// average elasticity is the elasticity of the row with "ALL" for model year
			Double avgElasticity = 0.0;
			for (Row avgelas : elasticity_result.collect()) {
				if (avgelas.getString(1).equals("ALL")) {
					avgElasticity =  avgelas.getDouble(6);
				}
			}

			logger.info("\n*********Pick Best iteration:"+i+" *******************"
					+ "\n Filter: " + filterString
					+ "\n Data Size: " + dataSize
					+ "\n Indep Var: "+ formula
					+ "\n pValue: "+ pvalue
					+ "\n coefficient: "+ regressionCoef
					+ "\n elasticity: "+ avgElasticity
					+ "\n **************************************************");
			
			if (bestdat == null && regressionCoef < 0){
		    	bestelasticity = avgElasticity;
				bestpvalue = pvalue;
				bestdat = dat;
				finalFormula = formula;
				msrpOutlier = MSRPRatioExclusions[i];
				shareOutlier = salesShareExlusions[i];
		    } else if (bestdat != null){
		    	//this is to identify a better set than just a negative coeff value
		    	if (regressionCoef < 0 && (
		    			//best set if average elasticity falls in the min, max range, and the existing is out of the range
		    			((bestelasticity < minMSRPElas || bestelasticity > maxMSRPElas) && (avgElasticity >= minMSRPElas && avgElasticity <= maxMSRPElas))
		    		
		    		//best set if existing out of range and new values also are out range and they are better than existing values
		    		||	((bestelasticity < minMSRPElas || bestelasticity > maxMSRPElas) && (avgElasticity < minMSRPElas && avgElasticity > maxMSRPElas)
		    			&& min(abs(avgElasticity - minMSRPElas), abs(avgElasticity-maxMSRPElas)) < min(abs(bestelasticity-minMSRPElas),abs(bestelasticity-maxMSRPElas)))
		    		
		    		//best set if the existing is IN the range and new values are better than existing
		    		|| ((bestelasticity >= minMSRPElas || bestelasticity <= maxMSRPElas) && (avgElasticity >= minMSRPElas && avgElasticity <= maxMSRPElas) && pvalue < bestpvalue) 
		    			))
		    	{
			    	bestelasticity = avgElasticity;
					bestpvalue = pvalue;
					bestdat = dat;
					finalFormula = formula;
					msrpOutlier = MSRPRatioExclusions[i];
					shareOutlier = salesShareExlusions[i];
		    	}
		    }
			elasticityTimeMillis += elt.endTimer();
		}

		logger.info("bestdata set is ::");
		bestdat.show();
		logger.info("\n*********End Pickbest Dataset *******************"
				+ "\n Total Time: " + pickBestTimer.endTimer() + " millis"
				+ "\n Total Dataframe to RDD conversion time: " + dataFrameToRDDTime + " millis"
				+ "\n Total Variable Selection Time: " + variableSelectionTimeMillis + " millis"
				+ "\n Total Regression Time: " + regressionTimeMillis + " millis"
				+ "\n Calculate Elasticity Time: "+ elasticityTimeMillis+ " millis"
				+ "\n Formula used for regression: "+ finalFormula
				+ "\n Best pvalue: "+ bestpvalue
				+ "\n Best elasticity: "+ bestelasticity
				+ "\n **************************************************");
		PickbestResult r = new PickbestResult(bestdat, bestpvalue, bestelasticity, finalFormula);
		return r;
	}
	

	public double getMSRPoutlier() {
		return msrpOutlier;
	}

	public double getshareoutlier() {
		return shareOutlier;
	}

	private List<String> getEffectiveVariables(JavaRDD<String> data, ModelParameters modelParameters) {

		logger.trace(" ******** Entering into variable selection ****** ");
		VariableSelection variableSelection = new VariableSelection();

		Map<String, Double> variablemap = null;
		try {
			variablemap = variableSelection.getVariables(data, modelParameters, sc, 10);
		} catch (AnalytixRuntimeException e1) {
			logger.error("Variable selection failed ***** " + e1.getMessage(), e1);
		}
		logger.trace("Variable Map" + MRMUtil.debugPrintMap(variablemap));

		List<String> indep_list = new ArrayList<>();
		// indep_list.add("dependent_variable");
		indep_list.addAll(variablemap.keySet());
		if (indep_list.contains("msrp_comp_ratio")) {
			indep_list.remove(indep_list.indexOf("msrp_comp_ratio"));
		}
		return indep_list;
	}

	//if series is Toyota then return toyota cftp, 
	//otherwise get Lexus
	private double getMSRPCoefInCFTP(String seriesName, ModelParameters modelParameters) {
		logger.debug("SeriesId:" + seriesName); 
		logger.debug( "Model Parameters Lexus Coeff: " + modelParameters.getLexusCFTPCoef() + " Toyota Coeff: " + modelParameters.getToyotaCFTPCoef());
		if (seriesName.equalsIgnoreCase("Lexus")) {
			return modelParameters.getLexusCFTPCoef();
		}
		return modelParameters.getToyotaCFTPCoef();
	}

	private double getDouble (Object o){
		if (o instanceof String){
			return Double.parseDouble((String) o);
		}else if(o instanceof Double){
			return (Double) o;
		}else if(o instanceof Integer){
			return new Double((Double) o);
		}
		else throw new RuntimeException("Cannot parse "+o + ":" + o.getClass().getName());
	}

}


