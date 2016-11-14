/**
 * 
 */
package com.toyota.analytix.mrm.msrp.bayesian;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.common.spark.SparkRunnableModel;
import com.toyota.analytix.common.util.MRMUtil;
import com.toyota.analytix.common.util.MessageConstants;
import com.toyota.analytix.common.util.QueryProperties;
import com.toyota.analytix.mrm.msrp.dataPrep.ModelParameters;

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
 * @author 
 *
 */
public class BayesianModeler implements Serializable {

	private static final long serialVersionUID = 3732367750815149L;
	
	static final Logger mrmDevLogger = Logger.getLogger(BayesianModeler.class);
	
	private ModelParameters modelParameters;
	private String series;

	public BayesianModeler(String series, ModelParameters modelParameters) {
		this.modelParameters = modelParameters;
		this.series = series;
	}

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
	 * @param sc
	 * @return
	 * @throws AnalytixRuntimeException
	 */
	public JavaRDD<BayesianResponsesDTO> calculateElasticityValues(
			JavaRDD<String> dataRDD) throws AnalytixRuntimeException {
		String me = "calculateElasticityValues";
		mrmDevLogger.info(me);

		JavaRDD<BayesianResponsesDTO> responses;
		BayesianModelerDTO elasticityDTO = readProperties();
		List<Double> meanValues;

		if (null == dataRDD || dataRDD.isEmpty()) {
			mrmDevLogger.error(MRMUtil.getValue(MessageConstants.EMPTY_DATA));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.EMPTY_DATA));
		} else {
			// Set the number of rows in the data set.
			elasticityDTO.setNumberOfObservations(dataRDD.count() - 1);

			// collect the header information.
			Map<String, Integer> headersInformation = MRMUtil.getHeadersInformation(dataRDD);
			elasticityDTO.setHeadersInformation(headersInformation);

			if (null == headersInformation || headersInformation.isEmpty()) {
				mrmDevLogger.error(MRMUtil
						.getValue(MessageConstants.COLUMN_HEADER_WARN));
				throw new AnalytixRuntimeException(
						MRMUtil.getValue(MessageConstants.COLUMN_HEADER_WARN));
			} else {
				// Generate the elasticity set.
				List<Float> elasticitySet = getElasticitySet(elasticityDTO);
				if (null == elasticitySet || elasticitySet.isEmpty()) {
					mrmDevLogger.error(MRMUtil
							.getValue(MessageConstants.EMPTY_ELAC_SET));
					throw new AnalytixRuntimeException(
							MRMUtil.getValue(MessageConstants.EMPTY_ELAC_SET));
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
			elasticityDTO = calculateLikelihoodValues(meanValues,
					elasticityDTO, dataRDD, series);

			// Calculate elasticity value.
			responses = calculateElasticity(elasticityDTO.getData(),
					elasticityDTO);
		}
		return responses;
	}

	/**
	 * This method calculates the likelihood values and posterior probability
	 * values based on the elasticity values.
	 * 
	 * @param data
	 * @param elasticityDTO
	 * @param sc
	 * @return
	 * @throws MRMVifNotFoundException
	 * @throws MRMNoDataFoundException
	 * @throws MRMMeanNotFoundException
	 * @throws MRMPostProbabilityListNotFoundException
	 * @throws MRMPriorProbabilityListNotFoundException
	 * @throws MRMLikelihoodListNotFoundException
	 */
	private JavaRDD<BayesianResponsesDTO> calculateElasticity(
			JavaRDD<BayesianPropertiesDTO> data,
			BayesianModelerDTO elasticityDTO){
		String me = "calculateElasticity";
		mrmDevLogger.info(me);
		JavaRDD<BayesianResponsesDTO> responses = null;

		// Collect the data from the input.
		List<BayesianPropertiesDTO> listValues = data.collect();
		if (null == listValues || listValues.isEmpty()) {
			mrmDevLogger.warn(MRMUtil.getValue(MessageConstants.NO_DATA));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.NO_DATA));
		} else {
			List<Double> coefList = elasticityDTO.getCoefList();
			double[] likelihoodList = elasticityDTO.getLikelihoodValues();
			if (null == coefList || coefList.isEmpty()
					|| null == likelihoodList) {
				mrmDevLogger.warn(MRMUtil.getValue(MessageConstants.COEF_WARN));
			} else {
				// Calculates the likelihood values.
				likelihoodList = calculateLikelihoodValues(coefList,
						listValues, likelihoodList, elasticityDTO);

				// Get the prior probability list.
				List<Double> priorProbList = elasticityDTO.getPriorProbList();
				if (null == priorProbList || priorProbList.isEmpty()
						|| checkProbabilityList(priorProbList) < 2) {
					mrmDevLogger.warn(MRMUtil
							.getValue(MessageConstants.PRIOR_PROB_WARN));
					throw new AnalytixRuntimeException(
							MRMUtil.getValue(MessageConstants.PRIOR_PROB_WARN));
				} else {
					// Sets posterior response.
					responses = setPosteriorResponse(elasticityDTO,
							likelihoodList, priorProbList, listValues,
							coefList);
				}
			}
		}
		return responses;
	}

	/**
	 * This method read input properties. These
	 * properties(min.MSRP.elas,max.MSRP
	 * .elas,MSRP.elas.radius,prior.MSRP.mean
	 * ,prior.MSRP.std.dev,prior.MSRP.
	 * std.dev,no.enough.data.std.dev.factor,stepsize,use.MSRP.pct.ratio) are
	 * required for this module. We should pass these properties values with
	 * these key names accordingly.
	 * 
	 * @param propertiesValues
	 * @return
	 * @throws MRMPropertiesNotFoundException
	 */
	private BayesianModelerDTO readProperties(){
		String me = "readProperties";
		mrmDevLogger.info(me);
		BayesianModelerDTO elasticityDTO = new BayesianModelerDTO();

			// Setting the properties.
		elasticityDTO.setMinElasticity(modelParameters.getMinMSRPElasticity());
		elasticityDTO.setMaxElasticity(modelParameters.getMaxMSRPElasticity());
		elasticityDTO.setElasticityRadius(modelParameters.getElasticityRadius());
		elasticityDTO.setPriorMean(modelParameters.getPriorMean());
		elasticityDTO.setPriorStdDev(modelParameters.getPriorStandardDeviation());
		elasticityDTO.setPriorAdjFactor(modelParameters.getPriorAdjFactor());
		elasticityDTO.setStdDevFactor(modelParameters.getStandardDeviationFactor());
		elasticityDTO.setStepSize(modelParameters.getStepSize());
		

		mrmDevLogger.debug("After Read Properties:" + ToStringBuilder.reflectionToString(elasticityDTO));
		return elasticityDTO;
	}

	/**
	 * This method gives you the elasticity set based on the minimum elasticity,
	 * maximum elasticity and step size. For example, Here minimum elasticity is
	 * 2, maximum elasticity is 6, based on the step size(0.1 here) this will
	 * give the elasticity set of values like (2.0,2.1 ... till 6.0) .
	 * 
	 * @param elasticityDTO
	 * @return
	 */
	private List<Float> getElasticitySet(BayesianModelerDTO elasticityDTO) {
		String me = "getElasticitySet";
		mrmDevLogger.info(me);
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
		mrmDevLogger.info("Min Elasticity" + minElasticity + "Max Elasticity"
				+ maxElasticity + "Step size:" + stepSize);

		// Generate the elasticity set.
		for (double val = minElasticity; val <= maxElasticity; val = val
				+ stepSize) {
			listOfDoubles.add((float) val);
		}
		mrmDevLogger.info("After " +  me +":" + ToStringBuilder.reflectionToString(elasticityDTO));
		return listOfDoubles;
	}

	/**
	 * 
	 * This method calculates the scale based on the input properties provided
	 * while calling this module. For calculate this , need to pass prior
	 * standard deviation, prior mean value to this method.
	 * 
	 * @param elasticityDTO
	 * @return
	 */
	private BayesianModelerDTO getScale(BayesianModelerDTO elasticityDTO) {
		String me = "getScale";
		mrmDevLogger.info(me);

		// Generates the scale value.
		double value = ((Math.pow(elasticityDTO.getPriorStdDev(), 2)) + (Math
				.pow(elasticityDTO.getPriorMean(), 2)) / 4);
		double sqrValue = Math.sqrt(value);
		double valueMean = elasticityDTO.getPriorMean() / 2;
		double scale = sqrValue + valueMean;
		elasticityDTO.setScale(scale);
		return elasticityDTO;
	}

	/**
	 * This method calculates the shape based on the prior mean and scale
	 * value.If we know the prior mean and scale value we can findout the shape
	 * based on this formula shape=1+(prior.mean/scale).
	 * 
	 * @param elasticityDTO
	 * @return
	 */
	private BayesianModelerDTO getShape(BayesianModelerDTO elasticityDTO) {
		String me = "getShape";
		mrmDevLogger.info(me);

		// Generates the shape based on the scale.
		// shape = 1 + prior.mean / scale
		double shape = 1 - (elasticityDTO.getPriorMean() / elasticityDTO
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
	 * @return
	 * @throws AnalytixRuntimeException
	 */
	private double getMean(JavaRDD<String> dataRDD, final int columnPosition)
			throws AnalytixRuntimeException {
		String me = "getMean";
		mrmDevLogger.info(me);
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
		mrmDevLogger.info(columnPosition);
		if (null == data || data.isEmpty()) {
			mrmDevLogger.error(MRMUtil.getValue(MessageConstants.EMPTY_DATA));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.EMPTY_DATA));
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
	 * @return
	 * @throws MRMNoDataFoundException
	 */
	private double getMeanVal(List<Double> listOfDoubleValues){
		String me = "getMeanVal";
		mrmDevLogger.info(me);
		double meanValue;

		if (null == listOfDoubleValues || listOfDoubleValues.isEmpty()) {
			mrmDevLogger.error(MRMUtil.getValue(MessageConstants.EMPTY_DATA));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.EMPTY_DATA));
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
	 * actual data set (MSRP_comp_ratio
	 * ,comp_MSRP,cftp,pred_value,msrp_comp_ratio,residual). As per this
	 * module, the dataset should contains this columns in the dataset to
	 * findout the mean values.
	 * 
	 * @param dataRDD
	 * @param headersInformation
	 * @return
	 * @throws AnalytixRuntimeException
	 */
	private List<Double> getColumnsMeanValue(JavaRDD<String> dataRDD,
			Map<String, Integer> headersInformation) throws AnalytixRuntimeException {
		String me = "getColumnsMeanValue";
		mrmDevLogger.info(me);

		// Taking the mean values for these column from the input data set.
		String[] pickupColumnNames = {
				MRMUtil.getValue(MessageConstants.MSRP_COMP_RATIO),
				MRMUtil.getValue(MessageConstants.COMP_MSRP),
				MRMUtil.getValue(MessageConstants.CFTP),
				MRMUtil.getValue(MessageConstants.PRED_VALUE),
				MRMUtil.getValue(MessageConstants.RESIDUAL) };
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
		mrmDevLogger.debug("Column Mean Values for MSRP_COMP_RATIO, COMP_MSRP, CFTP, PRED_VALUE, RESIDUAL");
		mrmDevLogger.debug(Arrays.toString(meanValues.toArray()));
		return meanValues;
	}

	/**
	 * This method gives you the prior probability values based on the density
	 * method from gamma distribution. This reads all the elasticity value and
	 * will provide the prior probability value based on the gamma distribution.
	 * 
	 * @param elasticityDTO
	 */
	private List<Double> getPriorProbability(BayesianModelerDTO elasticityDTO) {
		String me = "getPriorProbability";
		mrmDevLogger.info(me);
		List<Double> probList = new ArrayList<>();

		// Generates the prior probability list based on the gamma distribution.
		mrmDevLogger.debug("elasticityDTO.getShape():" + elasticityDTO.getShape() + ", elasticityDTO.getScale():" + elasticityDTO.getScale() );
		GammaDistribution gammaDistribution = new GammaDistribution(
				elasticityDTO.getShape(), elasticityDTO.getScale());
		List<Float> elasticValues = elasticityDTO.getElasticitySet();
		mrmDevLogger.debug("elasticValues:" + Arrays.toString(elasticValues.toArray()));
		for (Float value : elasticValues) {
			//if elasticity value is negative use positive of the same number. 
			//This happens when the appriori (bayesian assumed) probabilities are -ve. 
			//In that case we need to convert the values to positive. 
			//This may be specific for MSRP
			if (value <0) value = -value;
			Double d = gammaDistribution.density(value);
			mrmDevLogger.debug("gammaDistribution.density("+value+"):" + d);
			probList.add(d);
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
	 * @return
	 * @throws MRMElasticitiesNotFoundException
	 */
	 // Equivalent R Code
	// coef.set <- with(all.years.agg, elas.set / (avg_cftp / ((avg_pred_sales + avg_resid)* MSRP.coef.in.CFTP * avg_comp_msrp)))
	// Mean Values order 0-MSRP_COMP_RATIO, 1-COMP_MSRP, 2-CFTP, 3-PRED_VALUE, 4-RESIDUAL
	private List<Double> getCoefficientSet(BayesianModelerDTO elasticityDTO,
			List<Double> meanValues) {
		String me = "getCoefficientSet";
		mrmDevLogger.info(me);

		List<Float> listOfElasticVal = elasticityDTO.getElasticitySet();
		List<Double> coefficientList = new ArrayList<>();
		if (null == listOfElasticVal || listOfElasticVal.isEmpty()) {
			mrmDevLogger.info(MRMUtil.getValue(MessageConstants.ELAST_EXCE));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.ELAST_EXCE));
		} else {
			// Generating the coefficients list based on the respective values.
			for (Float value : listOfElasticVal) {
				try {
					mrmDevLogger.debug("Value:" + value);
					double denom1 = (meanValues.get(3) + meanValues.get(4)) * getMSRPCoefInCFTP() * meanValues.get(1);
					double denom = meanValues.get(2) / denom1;
					coefficientList.add(value / denom);
				} catch (Exception exception) {
					mrmDevLogger.error(MRMUtil.getValue(MessageConstants.ELAST_EXCE));
					throw new AnalytixRuntimeException(exception);
				}
			}
		}
		mrmDevLogger.debug("CoefficientList:" + Arrays.toString(coefficientList.toArray()));
		return coefficientList;
	}


	/**
	 * This method gives you the likelihood values based on the coefficient set.
	 * 
	 * @param elasticityDTO
	 * @param dataRDD
	 * @param avgRes
	 * @param avgMSRPCompRatio
	 * @param headersInformation
	 * @return
	 */
//	    likelihood <- rep(0, length(coef.set))
//	    for (i in 1 : length(coef.set)) {
//	      coef <- coef.set[i]
//	      dat$PRED_CENTERED_RESIDUAL <- coef * dat$CENTERED_MSRP_COMP_RATIO
//	      fit.std.dev <- sd(dat$PRED_CENTERED_RESIDUAL - dat$CENTERED_RESIDUAL)
//	      likelihood[i] <- -sum(with(dat, (PRED_CENTERED_RESIDUAL - CENTERED_RESIDUAL) ^ 2 / (2 * fit.std.dev ^ 2))) - log(2 * pi) * num.obs / 2 - num.obs * log(fit.std.dev)
//	    }
//	    post.prob <- exp(log(prior.prob) + likelihood)
//	    post.prob <- post.prob / sum(post.prob)
//	    max.index <- which.max(post.prob)
//	    MSRP.elas <- elas.set[max.index]
//	    dat$PRED_VALUE <- dat$PRED_VALUE + all.years.agg$avg_resid + coef.set[max.index] * dat$CENTERED_MSRP_COMP_RATIO

	// Mean Values order 0-MSRP_COMP_RATIO, 1-COMP_MSRP, 2-CFTP, 3-PRED_VALUE, 4-RESIDUAL
	private JavaRDD<BayesianPropertiesDTO> calculateLikelihood(
			final BayesianModelerDTO elasticityDTO,
			final JavaRDD<String> dataRDD, final double avgRes,
			final double avgMSRPCompRatio,
			final Map<String, Integer> headersInformation) {
		String me = "calculateLikelihood";
		mrmDevLogger.info(me);
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
							avgMSRPCompRatio);
					List<Double> coefList = elasticityDTO.getCoefList();
					for (Double coefObjValue : coefList) {
						int coefObjValueIndex = coefList.indexOf(coefObjValue);
						predCenteredResidual = coefList.get(coefObjValueIndex)
								* bayesianDetailsDTO.getCenteredAvgCompRatio();
						
						//TODO -  where is the sd function
						standardDeviationVal = predCenteredResidual
								- bayesianDetailsDTO.getCenteredResudual();
					}
					bayesianDetailsDTO
							.setPredcenteredResidual(predCenteredResidual);
					bayesianDetailsDTO.setDeviationVal(standardDeviationVal);
				} catch (NumberFormatException e) {
					mrmDevLogger.warn(e.getMessage());
				}
				mrmDevLogger.debug("Line:" + line + " Likelihood Object:" + ToStringBuilder.reflectionToString(bayesianDetailsDTO));
				return bayesianDetailsDTO;
			}
		});
	}

	/**
	 * This method sets the required column values for the DTO like residual,
	 * MSRP_comp_ratio and pre_value.
	 * 
	 * @param headersInformation
	 * @param parts
	 * @param bayesianDetailsDTO
	 * @param avgRes
	 * @param avgMSRPCompRatio
	 * @return
	 */
	//    dat$CENTERED_RESIDUAL <- dat$RESIDUAL - all.years.agg$avg_resid
	//    dat$CENTERED_MSRP_COMP_RATIO <- dat$MSRP_COMP_RATIO - all.years.agg$avg_msrp_comp_ratio

	private BayesianPropertiesDTO setParamValues(
			Map<String, Integer> headersInformation, String[] parts,
			BayesianPropertiesDTO bayesianDetailsDTO, double avgRes,
			double avgMSRPCompRatio) {
		//String me = "setParamValues";
		//mrmDevLogger.info(me);

		//mrmDevLogger.info("setParameterValues: headersInfo" + MRMUtil.debugPrintMap(headersInformation));
		
		// Setting the required properties.
		if (null != headersInformation) {
			mrmDevLogger.debug("MessageConstants.RESIDUAL: "+ MessageConstants.RESIDUAL);
			mrmDevLogger.debug("MRMUtil.getValue(MessageConstants.RESIDUAL): "+MRMUtil.getValue(MessageConstants.RESIDUAL));
			if (headersInformation.containsKey(MRMUtil.getValue(MessageConstants.RESIDUAL))) {
				double residual = Double
						.parseDouble(parts[headersInformation.get(MRMUtil.getValue(MessageConstants.RESIDUAL))]);
				bayesianDetailsDTO.setCenteredResudual(residual - avgRes);
			}
			if (headersInformation.containsKey(MRMUtil.getValue(MessageConstants.MSRP_COMP_RATIO))) {
				double msrpCompRatio = Double
						.parseDouble(parts[headersInformation.get(MRMUtil.getValue(MessageConstants.MSRP_COMP_RATIO))]);
				bayesianDetailsDTO.setCenteredAvgCompRatio(msrpCompRatio - avgMSRPCompRatio);
			}
			if (headersInformation.containsKey(MRMUtil.getValue(MessageConstants.PRED_VALUE))) {
				bayesianDetailsDTO.setPredValue(Double
						.parseDouble(parts[headersInformation.get(MRMUtil.getValue(MessageConstants.PRED_VALUE))]));
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
	 * @return
	 */
	private List<Double> postProbabilityList(List<Double> priorProbList,
			double[] likelihoodList) {
		String me = "postProbabilityList";
		mrmDevLogger.info(me);
		double[] posterList = new double[priorProbList.size()];
		
		// Reading the prior probability list.
		for ( int i =0; i< priorProbList.size(); i++) {
			Double priorProbValue = priorProbList.get(i);
			//int priorProbIndex = priorProbList.indexOf(priorProbValue);
			double postProb = Math.exp(Math.log(priorProbValue) + likelihoodList[i]);
			posterList[i] = postProb;
		}	
		mrmDevLogger.info("Posterior List:" + Arrays.toString(posterList));
		double sumOfPosterProb = 0.0;
		for (int i = 0; i < posterList.length; i++) {
			sumOfPosterProb = sumOfPosterProb + posterList[i];
		}
		mrmDevLogger.info("Sum Of Posterior Prob:" + sumOfPosterProb);
		
		// Generating the posterior probability list.
		List<Double> modifiedpostList = new ArrayList<>();
		for (double value : posterList) {
			double modValue = value / sumOfPosterProb;
			modifiedpostList.add(modValue);
		}
		mrmDevLogger.info("Final Posterior List:" + Arrays.toString(modifiedpostList.toArray()));
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
	 // Mean Values order 0-MSRP_COMP_RATIO, 1-COMP_MSRP, 2-CFTP, 3-PRED_VALUE, 4-RESIDUAL
	private List<Double> getPredValues(List<Double> modifiedpostList,
			List<BayesianPropertiesDTO> listValues, List<Double> coefList,
			List<Double> meanValues, List<Float> elasticitySet) {
		String me = "getPredValues";
		mrmDevLogger.info(me);
		double maxValue = java.util.Collections.max(modifiedpostList);
		int maxValIndex = modifiedpostList.indexOf(maxValue);
		float elasticityValue = elasticitySet.get(maxValIndex);
		mrmDevLogger.info("Maximum Value: " + maxValue + ": Max Value Index: "
				+ maxValIndex + " : Elasticity Value: " + elasticityValue);
		List<Double> modifiedPredList = new ArrayList<>();
		// Generates the pred values.
		for (BayesianPropertiesDTO objValue : listValues) {
			int objValueIndex = listValues.indexOf(objValue);
			if (objValueIndex > 0) {
				double modifiedPredValue = listValues.get(objValueIndex)
						.getPredValue()
						+ meanValues.get(4)
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
	 * @return
	 */
	private float getElastictyValue(List<Double> modifiedpostList,
			List<Float> elasticitySet) {
		String me = "getElastictyValue";
		mrmDevLogger.info(me);
		// Gives the Final elasticity value based on the input.
		double maxValue = java.util.Collections.max(modifiedpostList);
		int maxValIndex = modifiedpostList.indexOf(maxValue);
		float elasticityValue = elasticitySet.get(maxValIndex);
		mrmDevLogger.info("\n Elasticity Value: " + elasticityValue);
		return elasticityValue;
	}

	/**
	 * This method calculates the likelihood values.
	 * 
	 * @param coefList
	 * @param listValues
	 * @param likelihoodList
	 * @param elasticityDTO
	 * @return
	 */
	private double[] calculateLikelihoodValues(List<Double> coefList,
			List<BayesianPropertiesDTO> listValues, double[] likelihoodList,
			BayesianModelerDTO elasticityDTO) {
		String me = "calculateLikeliHoodValues";
		mrmDevLogger.info(me);
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
	 * @return
	 */
	private BayesianResponsesDTO setResponses(
			List<BayesianPropertiesDTO> listValues,
			List<Double> modifiedPredList) {
		String me = "setResponses";
		mrmDevLogger.info(me);
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
	 * @return
	 */
	private BayesianModelerDTO getScaleShape(BayesianModelerDTO elasticityDTO) {
		String me = "getScaleShape";
		mrmDevLogger.info(me);

		// Calculating the scale and shape.
		if (null != elasticityDTO) {
			elasticityDTO = getScale(elasticityDTO);
			elasticityDTO = getShape(elasticityDTO);
			mrmDevLogger.info("Scale: " + elasticityDTO.getScale()
					+ " : Shape: " + elasticityDTO.getShape());
		}
		mrmDevLogger.debug("After " +  me +":" + ToStringBuilder.reflectionToString(elasticityDTO));

		return elasticityDTO;
	}

	/**
	 * This method gives you the modified prior probability list.
	 * 
	 * @param elasticityDTO
	 * @return
	 */
	private BayesianModelerDTO modifyProbabilityList(
			BayesianModelerDTO elasticityDTO){
		String me = "modifyProbabilityList";
		mrmDevLogger.info(me);
		double sumOfProbability = 0.0;
		List<Double> probList = getPriorProbability(elasticityDTO);
		if (null == probList || probList.isEmpty()
				|| checkProbabilityList(probList) < 2) {
			throw new AnalytixRuntimeException(
					MRMUtil.getValue("PRIOR_PROB_LIST_EXCE"));
		} else {
			mrmDevLogger.info(probList);
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
			mrmDevLogger.info("Prior Probability List: " + modifiedprobList);
		}
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
	 * @return
	 * @throws MRMMeanNotFoundException
	 */
	private BayesianResponsesDTO setRequiredProperties(
			List<Double> modifiedpostList,
			List<BayesianPropertiesDTO> listValues, List<Double> coefList,
			List<Double> meanValues, List<Float> elasticitySet,
			double[] likelihoodList) {
		String me = "setRequiredProperties";
		mrmDevLogger.info(me);
		BayesianResponsesDTO bayesianResponsesDTO;

		if (null == meanValues || meanValues.isEmpty() || meanValues.size() < 5) {
			mrmDevLogger.warn(MRMUtil.getValue(MessageConstants.MEAN_ERROR));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.MEAN_ERROR));
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
		return bayesianResponsesDTO;
	}

	/**
	 * This method calculates the likelihood values.
	 * 
	 * @param meanValues
	 * @param elasticityDTO
	 * @param dataRDD
	 * @param sc
	 * @return
	 * @throws MRMMeanNotFoundException
	 * @throws MRMCoefficientsSetNotFoundException
	 * @throws MRMElasticitiesNotFoundException
	 */
	private BayesianModelerDTO calculateLikelihoodValues(
			List<Double> meanValues, BayesianModelerDTO elasticityDTO,
			JavaRDD<String> dataRDD, String series){
		String me = "calculateLikeliHoodValues";
		mrmDevLogger.info(me);

		// Checking the mean values.
		if (null == meanValues || meanValues.isEmpty() || meanValues.size() < 5) {
			mrmDevLogger.warn(MRMUtil.getValue(MessageConstants.MEAN_ERROR));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.MEAN_ERROR));
		} else {
			elasticityDTO.setMeanValues(meanValues);

			// Generate the coefficient list.
			List<Double> coefficientList = getCoefficientSet(elasticityDTO,
					meanValues);
			elasticityDTO.setCoefList(coefficientList);

			if (null == coefficientList || coefficientList.isEmpty()) {
				mrmDevLogger.warn(MRMUtil
						.getValue(MessageConstants.EMPTY_COEF_LIST));
				throw new AnalytixRuntimeException(
						MessageConstants.EMPTY_COEF_LIST);
			} else {
				double[] likelihoodVals = new double[coefficientList.size()];
				elasticityDTO.setLikelihoodValues(likelihoodVals);
				
				// Calculates the likelihood values.
				//Mean Values order 0-MSRP_COMP_RATIO, 1-COMP_MSRP, 2-CFTP, 3-PRED_VALUE, 4-RESIDUAL
				//calculateLikelihood(elasticityDTO,dataRDD, avgResidual, avgMSRPCompRatio,headersInformation)
				JavaRDD<BayesianPropertiesDTO> data = calculateLikelihood(
						elasticityDTO, dataRDD, meanValues.get(4),
						meanValues.get(0),
						elasticityDTO.getHeadersInformation());
				elasticityDTO.setData(data);
			}
		}
		return elasticityDTO;
	}

	/**
	 * This method checks the probability list is properly generated or not.
	 * 
	 * @param probList
	 * @return
	 */
	private int checkProbabilityList(List<Double> probList) {
		String me = "checkProbabilityList";
		mrmDevLogger.info(me + "List: " + Arrays.toString(probList.toArray()));

		// Initialize the set of values.
		Set<Double> setOfValues = new HashSet<>();
		if (null != probList && !probList.isEmpty()) {
			for (Double postValue : probList) {
				setOfValues.add(postValue);
			}
		}
		mrmDevLogger.info(me + "set: " + Arrays.toString(setOfValues.toArray()));
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
	 * @param sc
	 * @return
	 * @throws MRMPostProbabilityListNotFoundException
	 * @throws MRMMeanNotFoundException
	 */
	private JavaRDD<BayesianResponsesDTO> setPosteriorResponse(
			BayesianModelerDTO elasticityDTO, double[] likelihoodList,
			List<Double> priorProbList, List<BayesianPropertiesDTO> listValues,
			List<Double> coefList){
		String me = "setPosteriorResponse";
		mrmDevLogger.info(me);
		JavaRDD<BayesianResponsesDTO> responses;

		if (null == likelihoodList
				|| checkLikelihoodValuesCheck(likelihoodList) < 2) {
			mrmDevLogger.error(MRMUtil
					.getValue(MessageConstants.LIKELIHOOD_LIST_EXCE));
			throw new AnalytixRuntimeException(
					MRMUtil.getValue(MessageConstants.LIKELIHOOD_LIST_EXCE));
		} else {
			// Get the posterior probability list.
			List<Double> modifiedpostList = postProbabilityList(priorProbList,
					likelihoodList);
			if (null == modifiedpostList || modifiedpostList.isEmpty()
					|| checkProbabilityList(modifiedpostList) < 2) {
				mrmDevLogger.error(MRMUtil
						.getValue(MessageConstants.POST_PROB_WARN));
				throw new AnalytixRuntimeException(
						MRMUtil.getValue(MessageConstants.POST_PROB_WARN));
			} else {
				// Setting the final responses to the response DTO.
				BayesianResponsesDTO bayesianResponsesDTO = setRequiredProperties(
						modifiedpostList, listValues, coefList,
						elasticityDTO.getMeanValues(),
						elasticityDTO.getElasticitySet(), likelihoodList);

				// Pass responses back to client.
				responses = SparkRunnableModel.sc.parallelize(Arrays.asList(bayesianResponsesDTO));
			}
		}
		return responses;
	}

	/**
	 * This method checks the likelihood values list is properly generated or
	 * not.
	 * 
	 * @param likelihoodList
	 * @return
	 */
	private int checkLikelihoodValuesCheck(double[] likelihoodList) {
		String me = "checkLikelihoodValuesCheck";
		mrmDevLogger.info(me);

		// Initialize the set of values.
		Set<Double> setOfValues = new HashSet<>();
		if (null != likelihoodList) {
			for (Double postValue : likelihoodList) {
				setOfValues.add(postValue);
			}
		}
		return setOfValues.size();
	}
	
	//if series is Toyota then return toyota cftp, 
	//otherwise get Lexus
	private double getMSRPCoefInCFTP() {
		if (series.equalsIgnoreCase("Lexus")) {
			return modelParameters.getLexusCFTPCoef();
		}
		return modelParameters.getToyotaCFTPCoef();
	}
	
	public static void main (String[] args){
		QueryProperties.create("/Users//Desktop/Work/KaizenAnalytix/MSRPElasticityModel/Query.properties", null);
		BayesianModeler b = new BayesianModeler("Camry",null);
		BayesianModelerDTO bd= new BayesianModelerDTO();
		Float[] f = new Float[]{-4.5F, -4.4F, -4.3F, -4.2F, -4.1F, -4.0F, -3.9F, -3.8F, -3.7F, -3.6F, -3.5F, -3.4F, -3.3F, -3.2F, -3.1F, -3.0F, -2.9F, -2.8F, -2.7F, -2.6F, -2.5F, -2.4F, -2.3F, -2.2F, -2.1F, -2.0F};
		bd.setElasticitySet(Arrays.asList(f));
		Double[] d = new Double[]{0.9327121388666667, 24934.504087333335, 21679.839504333333, 0.14356337029999997, 5.204170427930421E-16};
		List<Double> l = Arrays.asList(d);
		double denom1 = (l.get(3) + l.get(4)) * 0.91775 * l.get(1);
		double denom =  l.get(2) / denom1;
		System.out.println("denom1:" + denom1 + " denom:"+ denom);
		
		List<Double> result = b.getCoefficientSet(bd, l);
		System.out.println("result:" + Arrays.toString(result.toArray()));
	}

}
