package bayesian;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import util.NetworkCommunicator;
import exceptions.MRMBaseException;

public class BasianFinalElasticity implements Serializable {

	private static final long serialVersionUID = 5967316686937996574L;

	// Initializing the logger.
	static final Logger mrmDevLogger = Logger
			.getLogger(BasianFinalElasticity.class);

	/**
	 * 
	 * @param bestdat
	 * @param parameterMap
	 * @return
	 * @throws MRMBaseException
	 */
	public double finalelasticity(DataFrame bestdat,
			Map<String, String> parameterMap) throws MRMBaseException {
		double finalElasticity = 0.0;
		// Calculating best elasticity
		String elasString = "";
		if (null == bestdat) {
			mrmDevLogger.info("Dataframe is empty.");
		} else {
			String[] colelas = bestdat.columns();
			for (int d = 0; d < colelas.length; d++) {
				if (!elasString.equals("")) {
					elasString = elasString + ",";
				}
				elasString = elasString + colelas[d];
			}
			JavaRDD<String> elascolheader = NetworkCommunicator
					.getJavaSparkContext().parallelize(
							Arrays.asList(elasString));
			JavaRDD<String> elasticityRDD = bestdat.toJavaRDD().map(
					new Function<Row, String>() {
						private static final long serialVersionUID = 6728818539648631098L;

						@Override
						public String call(Row row) throws Exception {
							return row.mkString(",");
						}
					});
			elascolheader = elascolheader.union(elasticityRDD);
			mrmDevLogger.info("data size?? " + elascolheader.count());

			// Basian function
			// Calling the method which will calculate the R square
			// value and VIF
			// Value based on input RDD.
			BayesianModeler elasticityCalculation = new BayesianModeler();
			JavaRDD<BayesianResponsesDTO> results = elasticityCalculation
					.calculateElasticityValues(elascolheader, parameterMap,
							NetworkCommunicator.getJavaSparkContext());

			if (null != results) {
				BayesianResponsesDTO bayesianResponsesDTO = results.first();
				mrmDevLogger.info(bayesianResponsesDTO
						.getCenteredResidualValues()
						+ " :"
						+ bayesianResponsesDTO.getCenteredAvgCompRatioValues()
						+ " : "
						+ bayesianResponsesDTO.getPredValues()
						+ bayesianResponsesDTO.getLikelihoodValues()
						+ bayesianResponsesDTO.getElastictyValue());
				finalElasticity = bayesianResponsesDTO.getElastictyValue();
			}
			mrmDevLogger.info("basian done:::: finalElasticity: "
					+ finalElasticity);
		}
		return finalElasticity;
	}
}
