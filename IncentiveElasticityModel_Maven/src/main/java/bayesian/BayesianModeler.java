/**
 * 
 */
package bayesian;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import util.MRMIndexes;
import exceptions.MRMBaseException;
import exceptions.MRMCoefficientsSetNotFoundException;
import exceptions.MRMElasticitiesNotFoundException;
import exceptions.MRMHeaderNotFoundException;
import exceptions.MRMLikelihoodListNotFoundException;
import exceptions.MRMMeanNotFoundException;
import exceptions.MRMNoDataFoundException;
import exceptions.MRMPostProbabilityListNotFoundException;
import exceptions.MRMPriorProbabilityListNotFoundException;
import exceptions.MRMPropertiesNotFoundException;
import exceptions.MRMVifNotFoundException;

/**
 * This class contains method which are calculates the elasticity value for best
 * data set. As per the process,this class reads the required properties,
 * calculates the scale and shape based on the input properties. This generates
 * the elasticity set based on the minimum elasticity, maximum elasticity and
 * step size parameters. From the input data set, it used to calculate the mean
 * values for required columns and calculates prior probabilities. As per the
 * elasticity set and the properties, this will take coefficients set, based on
 * the coefficients set this will calculates the likelihood values. We can take
 * the posterior distribution values based on the likelihood values.Finally, we
 * calculated the best elasticity value and required column values from this
 * module.
 * 
 * @author Naresh
 *
 */
public class BayesianModeler implements Serializable {

	private static final long serialVersionUID = 3732367750815149L;
	static final Logger mrmDevLogger = Logger.getLogger(BayesianModeler.class);

	/**
	 * This method calculates the best elasticity value as per input data set.
	 * As per the process, this method reads the required properties.This
	 * calculates the scale and shape based on the input properties. This
	 * generates the elasticity set based on the minimum elasticity, maximum
	 * elasticity and step size parameters. From the input data set, it used to
	 * calculate the mean values for required columns and calculates prior
	 * probabilities. As per the elasticity set and the properties, this will
	 * take coefficients set, based on the coefficients set this will calculates
	 * the likelihood values. We can take the posterior distribution values
	 * based on the likelihood values.Finally, we calculated the best elasticity
	 * values and required column values from this module.
	 * 
	 * @param dataRDD
	 * @param propertiesValues
	 * @param sparkContext
	 * @return responses
	 * @throws MRMBaseException
	 */
	public JavaRDD<BayesianResponsesDTO> calculateElasticityValues(
			JavaRDD<String> dataRDD, Map<String, String> propertiesValues,
			JavaSparkContext sparkContext) throws MRMBaseException {
		JavaRDD<BayesianResponsesDTO> responses;
		BayesianModelorDTO elasticityDTO = readProperties(propertiesValues);
		List<Double> meanValues;
		mrmDevLogger.warn("Bayesian calculate elasticity started at "
				+ new Date());
		mrmDevLogger.warn("Bayesian Input PropertiesValues: "
				+ propertiesValues);

		if (null == dataRDD || dataRDD.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			// Set the number of rows in the data set.
			elasticityDTO.setNumberOfObservations(dataRDD.count() - 1);

			// collect the header information.
			Map<String, Integer> headersInformation = MRMIndexes
					.getHeadersInformation(dataRDD);
			elasticityDTO.setHeadersInformation(headersInformation);

			if (null == headersInformation || headersInformation.isEmpty()) {
				mrmDevLogger.error(MRMIndexes.getValue("COLUMN_HEADER_WARN"));
				throw new MRMHeaderNotFoundException(
						MRMIndexes.getValue("COLUMN_HEADER_WARN"));
			} else {
				// Generate the elasticity set.
				List<Float> elasticitySet = getElasticitySet(elasticityDTO);
				if (null == elasticitySet || elasticitySet.isEmpty()) {
					mrmDevLogger.error(MRMIndexes.getValue("EMPTY_ELAC_SET"));
					throw new MRMElasticitiesNotFoundException(
							MRMIndexes.getValue("EMPTY_ELAC_SET"));
				} else {
					// Setting elasticity values to DTO.
					elasticityDTO.setElasticitySet(elasticitySet);

					// Calculate the scale and shape.
					elasticityDTO = getScaleShape(elasticityDTO);

					// Calculates the density probability values list.
					elasticityDTO = modifyProbabilityList(elasticityDTO);

					// Find out the column mean values.
					meanValues = getColumnsMeanValue(dataRDD,
							headersInformation);
				}
			}

			// Calculating the likelihood values and posterior probability
			// values.
			elasticityDTO = calculateLikeliHoodValues(meanValues,
					elasticityDTO, dataRDD);

			// Calculate elasticity value.
			responses = calculateElasticity(elasticityDTO.getData(),
					elasticityDTO, sparkContext);
		}
		mrmDevLogger.warn("Bayesian calculate elasticity ended at "
				+ new Date());
		return responses;
	}

	/**
	 * This method calculates the likelihood values and posterior probability
	 * values based on the elasticity values.
	 * 
	 * @param data
	 * @param elasticityDTO
	 * @param sparkContext
	 * @return responses
	 * @throws MRMVifNotFoundException
	 * @throws MRMNoDataFoundException
	 * @throws MRMMeanNotFoundException
	 * @throws MRMPostProbabilityListNotFoundException
	 * @throws MRMPriorProbabilityListNotFoundException
	 * @throws MRMLikelihoodListNotFoundException
	 */
	private JavaRDD<BayesianResponsesDTO> calculateElasticity(
			JavaRDD<BayesianPropertiesDTO> data,
			BayesianModelorDTO elasticityDTO, JavaSparkContext sparkContext)
			throws MRMVifNotFoundException, MRMNoDataFoundException,
			MRMMeanNotFoundException, MRMPostProbabilityListNotFoundException,
			MRMPriorProbabilityListNotFoundException,
			MRMLikelihoodListNotFoundException {
		JavaRDD<BayesianResponsesDTO> responses = null;

		// Collect the data from the input.
		List<BayesianPropertiesDTO> listValues = data.collect();
		if (null == listValues || listValues.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("NO_DATA"));
		} else {
			List<Double> coefList = elasticityDTO.getCoefList();
			double[] likelihoodList = elasticityDTO.getLikelihoodValues();
			if (null == coefList || coefList.isEmpty()
					|| null == likelihoodList) {
				mrmDevLogger.warn(MRMIndexes.getValue("COEF_WARN"));
			} else {
				// Calculates the likelihood values.
				likelihoodList = calculateLikeliHoodValues(coefList,
						listValues, likelihoodList, elasticityDTO);
				mrmDevLogger.warn("Bayesian likelihoodList: " + likelihoodList);

				// Get the prior probability list.
				List<Double> priorProbList = elasticityDTO.getPriorProbList();
				if (null == priorProbList || priorProbList.isEmpty()
						|| checkProbabilityList(priorProbList) < 2) {
					mrmDevLogger.error(MRMIndexes.getValue("PRIOR_PROB_WARN"));
					throw new MRMPriorProbabilityListNotFoundException(
							MRMIndexes.getValue("PRIOR_PROB_WARN"));
				} else {
					// Sets posterior response.
					responses = setPosteriorResponse(elasticityDTO,
							likelihoodList, priorProbList, listValues,
							coefList, sparkContext);
				}
			}
		}
		return responses;
	}

	/**
	 * This method read input properties. These
	 * properties(min.inctv.elas,max.inctv
	 * .elas,inctv.elas.radius,prior.inctv.mean
	 * ,prior.inctv.std.dev,prior.inctv.
	 * std.dev,no.enough.data.std.dev.factor,stepsize,use.inctv.pct.ratio) are
	 * required for this module. We should pass these properties values with
	 * these key names accordingly.
	 * 
	 * @param propertiesValues
	 * @return elasticityDTO
	 * @throws MRMPropertiesNotFoundException
	 */
	private BayesianModelorDTO readProperties(
			Map<String, String> propertiesValues)
			throws MRMPropertiesNotFoundException {
		BayesianModelorDTO elasticityDTO = new BayesianModelorDTO();
		if (null == propertiesValues || propertiesValues.isEmpty()
				|| propertiesValues.size() < 9) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_PROPERTIES"));
			throw new MRMPropertiesNotFoundException(
					MRMIndexes.getValue("NO_PROPERTIES"));
		} else {
			// Setting the properties.
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"MIN_INCTV_ELAS").trim())) {
				elasticityDTO.setMinElasticity(Double
						.parseDouble(propertiesValues
								.get(MRMIndexes.getValue("MIN_INCTV_ELAS"))
								.trim().toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"MAX_INCTV_ELAS").trim())) {
				elasticityDTO.setMaxElasticity(Double
						.parseDouble(propertiesValues
								.get(MRMIndexes.getValue("MAX_INCTV_ELAS"))
								.trim().toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"INCTV_ELAS_RADIUS").trim())) {
				elasticityDTO.setElasticityRadius(Double
						.parseDouble(propertiesValues
								.get(MRMIndexes.getValue("INCTV_ELAS_RADIUS"))
								.trim().toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"PRIOR_INCTV_MEAN").trim())) {
				elasticityDTO.setPriorMean(Double.parseDouble(propertiesValues
						.get(MRMIndexes.getValue("PRIOR_INCTV_MEAN")).trim()
						.toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"PRIOR_INCTV_STD_DEV").trim())) {
				elasticityDTO
						.setPriorStdDev(Double.parseDouble(propertiesValues
								.get(MRMIndexes.getValue("PRIOR_INCTV_STD_DEV"))
								.trim().toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"PRIOR_ADJ_FACTOR_NO_BEST_MODEL").trim())) {
				elasticityDTO
						.setPriorAdjFactor(Double.parseDouble(propertiesValues
								.get(MRMIndexes
										.getValue("PRIOR_ADJ_FACTOR_NO_BEST_MODEL"))
								.trim().toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"NO_ENOUGH_DATA_STD_DEV_FACTOR").trim())) {
				elasticityDTO
						.setStdDevFactor(Double.parseDouble(propertiesValues
								.get(MRMIndexes
										.getValue("NO_ENOUGH_DATA_STD_DEV_FACTOR"))
								.trim().toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue("STEPSIZE")
					.trim())) {
				elasticityDTO.setStepSize(Double.parseDouble(propertiesValues
						.get(MRMIndexes.getValue("STEPSIZE")).trim()
						.toLowerCase().trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"INCTV_PCT_RATIO").trim())) {
				elasticityDTO.setPctRatio(Double.parseDouble(propertiesValues
						.get(MRMIndexes.getValue("INCTV_PCT_RATIO")).trim()
						.toLowerCase().trim()));
			}
		}
		return elasticityDTO;
	}

	/**
	 * This method gives you the elasticity set based on the minimum elasticity,
	 * maximum elasticity and step size. For example, Here minimum elasticity is
	 * 2, maximum elasticity is 6, based on the step size(0.1 here) this will
	 * give the elasticity set of values like (2.0,2.1 ... till 6.0) .
	 * 
	 * @param elasticityDTO
	 * @return listOfDoubles
	 */
	private List<Float> getElasticitySet(BayesianModelorDTO elasticityDTO) {
		mrmDevLogger.warn("Bayesian getElasticitySet started at " + new Date());
		List<Float> listOfDoubles = new ArrayList<>();

		// Finding the maximum elasticity.
		double minElasticity = Math.max(
				elasticityDTO.getMinElasticity(),
				elasticityDTO.getPriorMean()
						- elasticityDTO.getElasticityRadius());

		// Finding the minimum elasticity.
		double maxElasticity = Math.min(
				elasticityDTO.getMaxElasticity(),
				elasticityDTO.getPriorMean()
						+ elasticityDTO.getElasticityRadius());
		double stepSize = elasticityDTO.getStepSize();

		// Generate the elasticity set.
		for (double val = minElasticity; val <= maxElasticity; val = val
				+ stepSize) {
			listOfDoubles.add((float) val);
		}
		mrmDevLogger.warn("Bayesian getElasticitySet ended at " + new Date());
		return listOfDoubles;
	}

	/**
	 * 
	 * This method calculates the scale based on the input properties provided
	 * while calling this module. For calculate this , need to pass prior
	 * standard deviation, prior mean value to this method.
	 * 
	 * @param elasticityDTO
	 * @return elasticityDTO
	 */
	private BayesianModelorDTO getScale(BayesianModelorDTO elasticityDTO) {
		// Generates the scale value.
		double value = ((Math.pow(elasticityDTO.getPriorStdDev(), 2)) + (Math
				.pow(elasticityDTO.getPriorMean(), 2)) / 4);
		double sqrValue = Math.sqrt(value);
		double valueMean = elasticityDTO.getPriorMean() / 2;
		double scale = sqrValue - valueMean;
		elasticityDTO.setScale(scale);
		return elasticityDTO;
	}

	/**
	 * This method calculates the shape based on the prior mean and scale
	 * value.If we know the prior mean and scale value we can findout the shape
	 * based on this formula shape=1+(prior.mean/scale).
	 * 
	 * @param elasticityDTO
	 * @return elasticityDTO
	 */
	private BayesianModelorDTO getShape(BayesianModelorDTO elasticityDTO) {
		// Generates the shape based on the scale.
		// shape = 1 + prior.mean / scale
		double shape = 1 + (elasticityDTO.getPriorMean() / elasticityDTO
				.getScale());
		elasticityDTO.setShape(shape);
		return elasticityDTO;
	}

	/**
	 * This method take the column value from the input data set based on the
	 * column index position and calculates the mean value. We uses Mean class
	 * from apache commons to get mean value based on the column value.
	 * 
	 * @param dataRDD
	 * @param columnPosition
	 * @return meanValue
	 * @throws MRMBaseException
	 */
	private double getMean(JavaRDD<String> dataRDD, final int columnPosition)
			throws MRMBaseException {
		double meanValue;

		JavaRDD<Double> data = dataRDD.map(new Function<String, Double>() {
			private static final long serialVersionUID = -6918192520144841841L;

			@Override
			public Double call(String line) {
				String[] parts = line.split(",");
				double columnValue = 0.0;
				try {
					columnValue = Double.parseDouble(parts[columnPosition]);
				} catch (NumberFormatException e) {
					mrmDevLogger.warn(e.getMessage());
				}
				return columnValue;
			}
		});
		if (null == data || data.isEmpty()) {
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			// Collect the data of particular column.
			List<Double> listOfDoubleValues = data.collect();

			// Get the mean value.
			meanValue = getMeanVal(listOfDoubleValues);
		}
		return meanValue;
	}

	/**
	 * This method returns the mean value of the particular column in data set.
	 * 
	 * @param listOfDoubleValues
	 * @return meanValue
	 * @throws MRMNoDataFoundException
	 */
	private double getMeanVal(List<Double> listOfDoubleValues)
			throws MRMNoDataFoundException {
		double meanValue;
		if (null == listOfDoubleValues || listOfDoubleValues.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			double[] values = new double[listOfDoubleValues.size() - 1];
			for (int i = 1; i < listOfDoubleValues.size(); i++) {
				if (null != listOfDoubleValues.get(i)) {
					double columnValue = listOfDoubleValues.get(i);
					values[i - 1] = columnValue;
				}
			}

			// It generates the mean value of particular column.
			Mean mean = new Mean();
			meanValue = mean.evaluate(values);
		}
		return meanValue;
	}

	/**
	 * This method gives you the mean values for particular columns from the
	 * actual data set (incentive_comp_ratio
	 * ,comp_incentive,cftp,pred_value,msrp_comp_ratio,residual). As per this
	 * module, the dataset should contains this columns in the dataset to
	 * findout the mean values.
	 * 
	 * @param dataRDD
	 * @param headersInformation
	 * @return meanValues
	 * @throws MRMBaseException
	 */
	private List<Double> getColumnsMeanValue(JavaRDD<String> dataRDD,
			Map<String, Integer> headersInformation) throws MRMBaseException {
		mrmDevLogger.warn("Bayesian getColumnsMeanValue started at "
				+ new Date());
		// Taking the mean values for these column from the input data set.
		String[] pickupColumnNames = {
				MRMIndexes.getValue("INCENTIVE_COMP_RATIO"),
				MRMIndexes.getValue("COMP_INCENTIVE"),
				MRMIndexes.getValue("CFTP"), MRMIndexes.getValue("PRED_VALUE"),
				MRMIndexes.getValue("MSRP_COMP_RATIO"),
				MRMIndexes.getValue("RESIDUAL") };
		double defaultMean = 0.0;
		List<Double> meanValues = new ArrayList<>();
		if (null != headersInformation) {
			for (int i = 0; i < pickupColumnNames.length; i++) {
				String trimColumn = pickupColumnNames[i].trim();
				if (headersInformation.containsKey(trimColumn)) {
					int columnIndex = headersInformation.get(trimColumn);

					// Get mean value for the respective column.
					double meanValue = getMean(dataRDD, columnIndex);
					meanValues.add(meanValue);
				} else {
					meanValues.add(defaultMean);
				}
			}
		}
		mrmDevLogger.warn("Bayesian meanValues: " + meanValues);
		mrmDevLogger
				.info("Bayesian getColumnsMeanValue ended at " + new Date());
		return meanValues;
	}

	/**
	 * This method gives you the prior probability values based on the density
	 * method from gamma distribution. This reads all the elasticity value and
	 * will provide the prior probability value based on the gamma distribution.
	 * 
	 * @param elasticityDTO
	 * @return probList
	 */
	private List<Double> getPriorProbability(BayesianModelorDTO elasticityDTO) {
		List<Double> probList = new ArrayList<>();

		// Generates the prior probability list based on the gamma distribution.
		GammaDistribution gammaDistribution = new GammaDistribution(
				elasticityDTO.getShape(), elasticityDTO.getScale());
		List<Float> elasticValues = elasticityDTO.getElasticitySet();
		for (Float value : elasticValues) {
			probList.add(gammaDistribution.density(value));
		}
		return probList;
	}

	/**
	 * This method gives you the coefficients set based on the mean values of
	 * particular columns and required properties. Coefficients will generate
	 * based on the elasticity values.
	 * 
	 * @param elasticityDTO
	 * @param meanValues
	 * @return coefficientList
	 * @throws MRMElasticitiesNotFoundException
	 */
	private List<Double> getCoefficientSet(BayesianModelorDTO elasticityDTO,
			List<Double> meanValues) throws MRMElasticitiesNotFoundException {
		List<Float> listOfElasticVal = elasticityDTO.getElasticitySet();
		List<Double> coefficientList = new ArrayList<>();
		if (null == listOfElasticVal || listOfElasticVal.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("ELAST_EXCE"));
			throw new MRMElasticitiesNotFoundException(
					MRMIndexes.getValue("ELAST_EXCE"));
		} else {
			// Generating the coefficients list based on the respective values.
			for (Float value : listOfElasticVal) {
				try {
					if (elasticityDTO.getPctRatio() == 1) {
						coefficientList.add(value
								/ (meanValues.get(2)
										/ (meanValues.get(3) * meanValues
												.get(1)) / meanValues.get(5)));
					} else {
						coefficientList.add(value
								/ (meanValues.get(2)
										/ (meanValues.get(3) * meanValues
												.get(1)) / 1));
					}
				} catch (Exception exception) {
					throw new MRMElasticitiesNotFoundException(
							MRMIndexes.getValue("ELAST_EXCE"));
				}
			}
		}
		return coefficientList;
	}

	/**
	 * This method gives you the likelihood values based on the coefficient set.
	 * 
	 * @param elasticityDTO
	 * @param dataRDD
	 * @param avgRes
	 * @param avgIncentCompRatio
	 * @param headersInformation
	 * @return
	 */
	private JavaRDD<BayesianPropertiesDTO> calculateLikelihood(
			final BayesianModelorDTO elasticityDTO,
			final JavaRDD<String> dataRDD, final double avgRes,
			final double avgIncentCompRatio,
			final Map<String, Integer> headersInformation) {
		return dataRDD.map(new Function<String, BayesianPropertiesDTO>() {
			private static final long serialVersionUID = 4962854265706157031L;

			@Override
			public BayesianPropertiesDTO call(String line) {
				BayesianPropertiesDTO bayesianDetailsDTO = new BayesianPropertiesDTO();
				String[] parts = line.split(",");
				double predCenteredResidual = 0.0;
				double standardDeviationVal = 0.0;
				try {
					// Calculating the likelihood values.
					bayesianDetailsDTO = setParamValues(headersInformation,
							parts, bayesianDetailsDTO, avgRes,
							avgIncentCompRatio);
					List<Double> coefList = elasticityDTO.getCoefList();
					for (Double coefObjValue : coefList) {
						int coefObjValueIndex = coefList.indexOf(coefObjValue);
						predCenteredResidual = coefList.get(coefObjValueIndex)
								* bayesianDetailsDTO.getCenteredAvgCompRatio();
						standardDeviationVal = predCenteredResidual
								- bayesianDetailsDTO.getCenteredResudual();
					}
					bayesianDetailsDTO
							.setPredcenteredResidual(predCenteredResidual);
					bayesianDetailsDTO.setDeviationVal(standardDeviationVal);
				} catch (NumberFormatException e) {
					mrmDevLogger.warn(e.getMessage());
				}
				return bayesianDetailsDTO;
			}
		});
	}

	/**
	 * This method sets the required column values for the DTO like residual,
	 * incentive_comp_ratio and pre_value.
	 * 
	 * @param headersInformation
	 * @param parts
	 * @param bayesianDetailsDTO
	 * @param avgRes
	 * @param avgIncentCompRatio
	 * @return bayesianDetailsDTO
	 */
	private BayesianPropertiesDTO setParamValues(
			Map<String, Integer> headersInformation, String[] parts,
			BayesianPropertiesDTO bayesianDetailsDTO, double avgRes,
			double avgIncentCompRatio) {
		// Setting the required properties.
		if (null != headersInformation) {
			if (headersInformation.containsKey(MRMIndexes.getValue("RESIDUAL"))) {
				double residual = Double.parseDouble(parts[headersInformation
						.get(MRMIndexes.getValue("RESIDUAL"))]);
				bayesianDetailsDTO.setCenteredResudual(residual - avgRes);
			}
			if (headersInformation.containsKey(MRMIndexes
					.getValue("INCENTIVE_COMP_RATIO"))) {
				double incentiveCompRatio = Double
						.parseDouble(parts[headersInformation.get(MRMIndexes
								.getValue("INCENTIVE_COMP_RATIO"))]);
				bayesianDetailsDTO.setCenteredAvgCompRatio(incentiveCompRatio
						- avgIncentCompRatio);
			}
			if (headersInformation.containsKey(MRMIndexes
					.getValue("PRED_VALUE"))) {
				bayesianDetailsDTO.setPredValue(Double
						.parseDouble(parts[headersInformation.get(MRMIndexes
								.getValue("PRED_VALUE"))]));
			}
		}
		return bayesianDetailsDTO;
	}

	/**
	 * This method will give the posterior probability values based on the prior
	 * probability list.
	 * 
	 * @param priorProbList
	 * @param likelihoodList
	 * @return modifiedpostList
	 */
	private List<Double> postProbabilityList(List<Double> priorProbList,
			double[] likelihoodList) {
		double[] posterList = new double[priorProbList.size()];

		// Reading the prior probability list.
		for (Double priorProbValue : priorProbList) {
			int priorProbIndex = priorProbList.indexOf(priorProbValue);
			double postProb = Math.exp(Math.log(priorProbList
					.get(priorProbIndex)) + likelihoodList[priorProbIndex]);
			posterList[priorProbIndex] = postProb;
		}
		double sumOfPosterProb = 0.0;
		for (int i = 0; i < posterList.length; i++) {
			sumOfPosterProb = sumOfPosterProb + posterList[i];
		}
		// Generating the posterior probability list.
		List<Double> modifiedpostList = new ArrayList<>();
		for (double value : posterList) {
			double modValue = value / sumOfPosterProb;
			modifiedpostList.add(modValue);
		}
		return modifiedpostList;
	}

	/**
	 * This method calculates the pred values based on the coefficient list and
	 * centered average comp ratio values.
	 * 
	 * @param modifiedpostList
	 * @param listValues
	 * @param coefList
	 * @param meanValues
	 */
	private List<Double> getPredValues(List<Double> modifiedpostList,
			List<BayesianPropertiesDTO> listValues, List<Double> coefList,
			List<Double> meanValues, List<Float> elasticitySet) {
		double maxValue = java.util.Collections.max(modifiedpostList);
		int maxValIndex = modifiedpostList.indexOf(maxValue);
		float elasticityValue = elasticitySet.get(maxValIndex);
		mrmDevLogger.warn("Maximum Value: " + maxValue + ": Max Value Index: "
				+ maxValIndex + " : Elasticity Value: " + elasticityValue);
		List<Double> modifiedPredList = new ArrayList<>();
		// Generates the pred values.
		for (BayesianPropertiesDTO objValue : listValues) {
			int objValueIndex = listValues.indexOf(objValue);
			if (objValueIndex > 0) {
				double modifiedPredValue = listValues.get(objValueIndex)
						.getPredValue()
						+ meanValues.get(5)
						+ (coefList.get(maxValIndex) * listValues.get(
								objValueIndex).getCenteredAvgCompRatio());
				modifiedPredList.add(modifiedPredValue);
			}
		}
		return modifiedPredList;
	}

	/**
	 * This method gives you the best elasticity value based on the posterior
	 * probability list and will pickup that value from the elasticity set.
	 * 
	 * @param modifiedpostList
	 * @param elasticitySet
	 * @return elasticityValue
	 */
	private float getElastictyValue(List<Double> modifiedpostList,
			List<Float> elasticitySet) {
		// Gives the Final elasticity value based on the input.
		double maxValue = java.util.Collections.max(modifiedpostList);
		int maxValIndex = modifiedpostList.indexOf(maxValue);
		float elasticityValue = elasticitySet.get(maxValIndex);
		mrmDevLogger.warn("\n Elasticity Value: " + elasticityValue);
		return elasticityValue;
	}

	/**
	 * This method calculates the likelihood values.
	 * 
	 * @param coefList
	 * @param listValues
	 * @param likelihoodList
	 * @param elasticityDTO
	 * @return likelihoodList
	 */
	private double[] calculateLikeliHoodValues(List<Double> coefList,
			List<BayesianPropertiesDTO> listValues, double[] likelihoodList,
			BayesianModelorDTO elasticityDTO) {
		StandardDeviation deviation = new StandardDeviation();

		// Generates the likelihood values using the coefficient list.
		for (int i = 0; i < coefList.size(); i++) {
			double likelihoodVal = 0.0;
			double coef = coefList.get(i);
			double[] predValues = new double[listValues.size() - 1];
			double[] deviationDiff = new double[predValues.length];
			for (BayesianPropertiesDTO objValue : listValues) {
				int objValueIndex = listValues.indexOf(objValue);
				if (objValueIndex > 0) {
					predValues[objValueIndex - 1] = coef
							* listValues.get(objValueIndex)
									.getCenteredAvgCompRatio();
				}
			}
			for (int j = 0; j < predValues.length; j++) {
				deviationDiff[j] = predValues[j]
						- listValues.get(j + 1).getCenteredResudual();
			}
			// Calculating the standard deviation.
			double standardDev = deviation.evaluate(deviationDiff);
			for (int j = 1; j < listValues.size(); j++) {
				likelihoodVal = likelihoodVal
						+ (Math.pow(predValues[j - 1]
								- listValues.get(j).getCenteredResudual(), 2) / (2 * Math
								.pow(standardDev, 2)));
			}
			double likelihoodVal2 = (Math.log(2 * 3.14159)
					* elasticityDTO.getNumberOfObservations() / 2)
					+ (elasticityDTO.getNumberOfObservations() * Math
							.log(standardDev));
			likelihoodList[i] = -likelihoodVal - likelihoodVal2;
		}
		return likelihoodList;
	}

	/**
	 * This method sets the output values.
	 * 
	 * @param listValues
	 * @param modifiedPredList
	 * @return bayesianResponsesDTO
	 */
	private BayesianResponsesDTO setResponses(
			List<BayesianPropertiesDTO> listValues,
			List<Double> modifiedPredList) {
		BayesianResponsesDTO bayesianResponsesDTO = new BayesianResponsesDTO();
		List<Double> centeredResidual = new ArrayList<>();
		List<Double> centeredAvgCompRatio = new ArrayList<>();
		// Setting the responses.
		for (BayesianPropertiesDTO bayesianPropertiesDTO : listValues) {
			int objValueIndex = listValues.indexOf(bayesianPropertiesDTO);
			if (objValueIndex > 0) {
				centeredResidual.add(bayesianPropertiesDTO
						.getCenteredResudual());
				centeredAvgCompRatio.add(bayesianPropertiesDTO
						.getCenteredAvgCompRatio());
			}
		}
		bayesianResponsesDTO.setCenteredResidualValues(centeredResidual);
		bayesianResponsesDTO
				.setCenteredAvgCompRatioValues(centeredAvgCompRatio);
		bayesianResponsesDTO.setPredValues(modifiedPredList);
		return bayesianResponsesDTO;
	}

	/**
	 * This method calculates the scale and shape values.
	 * 
	 * @param elasticityDTO
	 * @return elasticityDTO
	 */
	private BayesianModelorDTO getScaleShape(BayesianModelorDTO elasticityDTO) {
		mrmDevLogger.warn("Bayesian getScaleShape started at " + new Date());
		// Calculating the scale and shape.
		if (null != elasticityDTO) {
			elasticityDTO = getScale(elasticityDTO);
			elasticityDTO = getShape(elasticityDTO);
		}
		mrmDevLogger.warn("Bayesian getScaleShape ended at " + new Date());
		return elasticityDTO;
	}

	/**
	 * This method gives you the modified prior probability list.
	 * 
	 * @param elasticityDTO
	 * @return elasticityDTO
	 */
	private BayesianModelorDTO modifyProbabilityList(
			BayesianModelorDTO elasticityDTO)
			throws MRMPriorProbabilityListNotFoundException {
		mrmDevLogger.warn("Bayesian modifyProbabilityList started at "
				+ new Date());
		double sumOfProbability = 0.0;
		List<Double> probList = getPriorProbability(elasticityDTO);

		if (null == probList || probList.isEmpty()
				|| checkProbabilityList(probList) < 2) {
			mrmDevLogger.error(MRMIndexes.getValue("PRIOR_PROB_LIST_EXCE"));
			throw new MRMPriorProbabilityListNotFoundException(
					MRMIndexes.getValue("PRIOR_PROB_LIST_EXCE"));
		} else {
			for (Double probValue : probList) {
				sumOfProbability = sumOfProbability + probValue;
			}

			// Generating the modified probability list.
			List<Double> modifiedprobList = new ArrayList<>();
			for (double value : probList) {
				double modValue = value / sumOfProbability;
				modifiedprobList.add(modValue);
			}
			elasticityDTO.setPriorProbList(modifiedprobList);
			mrmDevLogger.warn("Bayesian modifiedprobList: " + modifiedprobList);
		}
		mrmDevLogger.warn("Bayesian modifyProbabilityList ended at "
				+ new Date());
		return elasticityDTO;
	}

	/**
	 * This method gives you the final responses from the model. like best
	 * elasticity value, pred values.
	 * 
	 * @param modifiedpostList
	 * @param listValues
	 * @param coefList
	 * @param meanValues
	 * @param elasticitySet
	 * @param likelihoodList
	 * @return bayesianResponsesDTO
	 * @throws MRMMeanNotFoundException
	 */
	private BayesianResponsesDTO setRequiredProperties(
			List<Double> modifiedpostList,
			List<BayesianPropertiesDTO> listValues, List<Double> coefList,
			List<Double> meanValues, List<Float> elasticitySet,
			double[] likelihoodList) throws MRMMeanNotFoundException {
		BayesianResponsesDTO bayesianResponsesDTO;
		mrmDevLogger.warn("Bayesian setRequiredProperties started at "
				+ new Date());
		if (null == meanValues || meanValues.isEmpty() || meanValues.size() < 6) {
			mrmDevLogger.error(MRMIndexes.getValue("MEAN_ERROR"));
			throw new MRMMeanNotFoundException(
					MRMIndexes.getValue("MEAN_ERROR"));
		} else {
			// Setting the required properties.
			List<Double> modifiedPredList = getPredValues(modifiedpostList,
					listValues, coefList, meanValues, elasticitySet);
			float elastictyValue = getElastictyValue(modifiedpostList,
					elasticitySet);
			bayesianResponsesDTO = setResponses(listValues, modifiedPredList);
			bayesianResponsesDTO.setElastictyValue(elastictyValue);
			bayesianResponsesDTO.setLikelihoodValues(likelihoodList);
		}
		mrmDevLogger.warn("Bayesian setRequiredProperties ended at "
				+ new Date());
		return bayesianResponsesDTO;
	}

	/**
	 * This method calculates the likelihood values.
	 * 
	 * @param meanValues
	 * @param elasticityDTO
	 * @param dataRDD
	 * @return elasticityDTO
	 * @throws MRMMeanNotFoundException
	 * @throws MRMCoefficientsSetNotFoundException
	 * @throws MRMElasticitiesNotFoundException
	 */
	private BayesianModelorDTO calculateLikeliHoodValues(
			List<Double> meanValues, BayesianModelorDTO elasticityDTO,
			JavaRDD<String> dataRDD) throws MRMMeanNotFoundException,
			MRMCoefficientsSetNotFoundException,
			MRMElasticitiesNotFoundException {
		mrmDevLogger.warn("Bayesian calculateLikeliHoodValues started at "
				+ new Date());
		// Checking the mean values.
		if (null == meanValues || meanValues.isEmpty() || meanValues.size() < 6) {
			mrmDevLogger.error(MRMIndexes.getValue("MEAN_ERROR"));
			throw new MRMMeanNotFoundException(
					MRMIndexes.getValue("MEAN_ERROR"));
		} else {
			elasticityDTO.setMeanValues(meanValues);

			// Generate the coefficient list.
			List<Double> coefficientList = getCoefficientSet(elasticityDTO,
					meanValues);
			elasticityDTO.setCoefList(coefficientList);
			if (null == coefficientList || coefficientList.isEmpty()) {
				mrmDevLogger.error(MRMIndexes.getValue("EMPTY_COEF_LIST"));
				throw new MRMCoefficientsSetNotFoundException(
						MRMIndexes.getValue("EMPTY_COEF_LIST"));
			} else {
				double[] likelihoodVals = new double[coefficientList.size()];
				elasticityDTO.setLikelihoodValues(likelihoodVals);

				// Calculates the likelihood values.
				JavaRDD<BayesianPropertiesDTO> data = calculateLikelihood(
						elasticityDTO, dataRDD, meanValues.get(5),
						meanValues.get(0),
						elasticityDTO.getHeadersInformation());
				elasticityDTO.setData(data);
			}
		}
		mrmDevLogger.warn("Bayesian calculateLikeliHoodValues ended at "
				+ new Date());
		return elasticityDTO;
	}

	/**
	 * This method checks the probability list is properly generated or not.
	 * 
	 * @param probList
	 */
	private int checkProbabilityList(List<Double> probList) {
		// Initialize the set of values.
		Set<Double> setOfValues = new HashSet<>();
		if (null != probList && !probList.isEmpty()) {
			for (Double postValue : probList) {
				setOfValues.add(postValue);
			}
		}
		return setOfValues.size();
	}

	/**
	 * This method sets the posterior response.
	 * 
	 * @param elasticityDTO
	 * @param likelihoodList
	 * @param priorProbList
	 * @param listValues
	 * @param coefList
	 * @param sparkContext
	 * @return responses
	 * @throws MRMPostProbabilityListNotFoundException
	 * @throws MRMMeanNotFoundException
	 */
	private JavaRDD<BayesianResponsesDTO> setPosteriorResponse(
			BayesianModelorDTO elasticityDTO, double[] likelihoodList,
			List<Double> priorProbList, List<BayesianPropertiesDTO> listValues,
			List<Double> coefList, JavaSparkContext sparkContext)
			throws MRMPostProbabilityListNotFoundException,
			MRMMeanNotFoundException, MRMLikelihoodListNotFoundException {
		JavaRDD<BayesianResponsesDTO> responses = null;
		mrmDevLogger.warn("Bayesian setPosteriorResponse started at "
				+ new Date());
		if (null == likelihoodList
				|| checkLikelihoodValuesCheck(likelihoodList) < 2) {
			mrmDevLogger.warn(MRMIndexes.getValue("LIKELIHOOD_LIST_EXCE"));
		} else {
			// Get the posterior probability list.
			List<Double> modifiedpostList = postProbabilityList(priorProbList,
					likelihoodList);
			if (null == modifiedpostList || modifiedpostList.isEmpty()
					|| checkProbabilityList(modifiedpostList) < 2) {
				mrmDevLogger.warn(MRMIndexes.getValue("POST_PROB_WARN"));
			} else {
				// Setting the final responses to the response DTO.
				BayesianResponsesDTO bayesianResponsesDTO = setRequiredProperties(
						modifiedpostList, listValues, coefList,
						elasticityDTO.getMeanValues(),
						elasticityDTO.getElasticitySet(), likelihoodList);

				// Pass responses back to client.
				responses = sparkContext.parallelize(Arrays
						.asList(bayesianResponsesDTO));
			}
		}
		mrmDevLogger.warn("Bayesian setPosteriorResponse ended at "
				+ new Date());
		return responses;
	}

	/**
	 * This method checks the likelihood values list is properly generated or
	 * not.
	 * 
	 * @param likelihoodList
	 */
	private int checkLikelihoodValuesCheck(double[] likelihoodList) {
		// Initialize the set of values.
		Set<Double> setOfValues = new HashSet<>();
		if (null != likelihoodList) {
			for (Double postValue : likelihoodList) {
				setOfValues.add(postValue);
			}
		}
		return setOfValues.size();
	}
}
