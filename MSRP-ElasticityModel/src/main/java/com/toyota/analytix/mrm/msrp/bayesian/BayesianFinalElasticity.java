package com.toyota.analytix.mrm.msrp.bayesian;

import static com.toyota.analytix.common.spark.SparkRunnableModel.sc;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.mrm.msrp.dataPrep.ModelParameters;

public class BayesianFinalElasticity implements Serializable{

	private static final Logger logger = Logger.getLogger(BayesianFinalElasticity.class);
	
	private static final long serialVersionUID = 5967316686937996574L;

	public double finalelasticity(DataFrame bestdat, String series, ModelParameters modelParameters)
			throws AnalytixRuntimeException {
		
		// Calculating best elasticity
		String elasString = "";

		// get the comma separated column list for bestDat
		String[] colelas = bestdat.columns();
		for (int d = 0; d < colelas.length; d++) {
			if (!elasString.equals("")) {
				elasString = elasString + ",";
			}
			elasString = elasString + colelas[d];
		}
		
		logger.debug("Comma Separated Columns for dataset for Bayesian:\n" + elasString);

		JavaRDD<String> elascolheader = null;
		if (null != sc) {
			elascolheader = sc.parallelize(Arrays.asList(elasString));
		}

		// convert bestDat dataframe to a comma separated RDD
		JavaRDD<String> elasticityRDD = bestdat.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 6078077496917116728L;

			public String call(Row row) throws Exception {
				logger.debug(row.mkString(","));
				return row.mkString(",");
			}
		});

		// Add the header csv to the data
		elascolheader = elascolheader.union(elasticityRDD);

		// Printing the data
		logger.debug("RDD before Bayesian:::");
		for (String dta : elascolheader.collect()) {
			logger.debug(dta);
		}

		// Print data size - will be one more -
		logger.debug("data size?? " + elascolheader.count());

		// Basian function
		// Calling the method which will calculate the R square
		// value and VIF
		// Value based on input RDD.
		BayesianModeler elasticityCalculation = new BayesianModeler(series, modelParameters);

		JavaRDD<BayesianResponsesDTO> results = elasticityCalculation.calculateElasticityValues(elascolheader);

		BayesianResponsesDTO bayesianResponsesDTO = results.first();
		logger.debug("bayesian done:::: ");
		logger.debug(bayesianResponsesDTO.getCenteredResidualValues() + " :"
				+ bayesianResponsesDTO.getCenteredAvgCompRatioValues() + " : " + bayesianResponsesDTO.getPredValues()
				+ bayesianResponsesDTO.getLikelihoodValues() + bayesianResponsesDTO.getElastictyValue());

		return bayesianResponsesDTO.getElastictyValue();

	}
}
