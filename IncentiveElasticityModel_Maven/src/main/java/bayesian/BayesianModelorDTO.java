/**
 * 
 */
package bayesian;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;

/**
 * This class contains the fields required for the bayesian module. Here we have
 * setters and getters for these fields. If we need any properties from this ,
 * we can get using getters, if we want to set, we can set the properties using
 * setters.
 * 
 * @author Naresh
 *
 */
public class BayesianModelorDTO implements Serializable {

	private static final long serialVersionUID = 6535529006806091806L;

	// Initializing the required attributes.
	private double minElasticity;
	private double maxElasticity;
	private double elasticityRadius;
	private double priorMean;
	private double priorStdDev;
	private double priorAdjFactor;
	private double stdDevFactor;
	private double stepSize;
	private double pctRatio;
	private List<Float> elasticitySet;
	private double scale;
	private double shape;
	private long numberOfObservations;
	private List<Double> coefList;
	private List<Double> priorProbList;
	private List<Double> meanValues;
	private double[] likelihoodValues;
	private Map<String, Integer> headersInformation;
	private JavaRDD<BayesianPropertiesDTO> data;

	/**
	 * @return the minElasticity
	 */
	public double getMinElasticity() {
		return minElasticity;
	}

	/**
	 * @param minElasticity
	 *            the minElasticity to set
	 */
	public void setMinElasticity(double minElasticity) {
		this.minElasticity = minElasticity;
	}

	/**
	 * @return the maxElasticity
	 */
	public double getMaxElasticity() {
		return maxElasticity;
	}

	/**
	 * @param maxElasticity
	 *            the maxElasticity to set
	 */
	public void setMaxElasticity(double maxElasticity) {
		this.maxElasticity = maxElasticity;
	}

	/**
	 * @return the elasticityRadius
	 */
	public double getElasticityRadius() {
		return elasticityRadius;
	}

	/**
	 * @param elasticityRadius
	 *            the elasticityRadius to set
	 */
	public void setElasticityRadius(double elasticityRadius) {
		this.elasticityRadius = elasticityRadius;
	}

	/**
	 * @return the priorMean
	 */
	public double getPriorMean() {
		return priorMean;
	}

	/**
	 * @param priorMean
	 *            the priorMean to set
	 */
	public void setPriorMean(double priorMean) {
		this.priorMean = priorMean;
	}

	/**
	 * @return the priorStdDev
	 */
	public double getPriorStdDev() {
		return priorStdDev;
	}

	/**
	 * @param priorStdDev
	 *            the priorStdDev to set
	 */
	public void setPriorStdDev(double priorStdDev) {
		this.priorStdDev = priorStdDev;
	}

	/**
	 * @return the priorAdjFactor
	 */
	public double getPriorAdjFactor() {
		return priorAdjFactor;
	}

	/**
	 * @param priorAdjFactor
	 *            the priorAdjFactor to set
	 */
	public void setPriorAdjFactor(double priorAdjFactor) {
		this.priorAdjFactor = priorAdjFactor;
	}

	/**
	 * @return the stdDevFactor
	 */
	public double getStdDevFactor() {
		return stdDevFactor;
	}

	/**
	 * @param stdDevFactor
	 *            the stdDevFactor to set
	 */
	public void setStdDevFactor(double stdDevFactor) {
		this.stdDevFactor = stdDevFactor;
	}

	/**
	 * @return the stepSize
	 */
	public double getStepSize() {
		return stepSize;
	}

	/**
	 * @param stepSize
	 *            the stepSize to set
	 */
	public void setStepSize(double stepSize) {
		this.stepSize = stepSize;
	}

	/**
	 * @return the pctRatio
	 */
	public double getPctRatio() {
		return pctRatio;
	}

	/**
	 * @param pctRatio
	 *            the pctRatio to set
	 */
	public void setPctRatio(double pctRatio) {
		this.pctRatio = pctRatio;
	}

	/**
	 * @return the elasticitySet
	 */
	public List<Float> getElasticitySet() {
		return elasticitySet;
	}

	/**
	 * @param elasticitySet
	 *            the elasticitySet to set
	 */
	public void setElasticitySet(List<Float> elasticitySet) {
		this.elasticitySet = elasticitySet;
	}

	/**
	 * @return the scale
	 */
	public double getScale() {
		return scale;
	}

	/**
	 * @param scale
	 *            the scale to set
	 */
	public void setScale(double scale) {
		this.scale = scale;
	}

	/**
	 * @return the shape
	 */
	public double getShape() {
		return shape;
	}

	/**
	 * @param shape
	 *            the shape to set
	 */
	public void setShape(double shape) {
		this.shape = shape;
	}

	/**
	 * @return the numberOfObservations
	 */
	public long getNumberOfObservations() {
		return numberOfObservations;
	}

	/**
	 * @param numberOfObservations
	 *            the numberOfObservations to set
	 */
	public void setNumberOfObservations(long numberOfObservations) {
		this.numberOfObservations = numberOfObservations;
	}

	/**
	 * @return the coefList
	 */
	public List<Double> getCoefList() {
		return coefList;
	}

	/**
	 * @param coefList
	 *            the coefList to set
	 */
	public void setCoefList(List<Double> coefList) {
		this.coefList = coefList;
	}

	/**
	 * @return the priorProbList
	 */
	public List<Double> getPriorProbList() {
		return priorProbList;
	}

	/**
	 * @param priorProbList
	 *            the priorProbList to set
	 */
	public void setPriorProbList(List<Double> priorProbList) {
		this.priorProbList = priorProbList;
	}

	/**
	 * @return the meanValues
	 */
	public List<Double> getMeanValues() {
		return meanValues;
	}

	/**
	 * @param meanValues
	 *            the meanValues to set
	 */
	public void setMeanValues(List<Double> meanValues) {
		this.meanValues = meanValues;
	}

	/**
	 * @return the likelihoodValues
	 */
	public double[] getLikelihoodValues() {
		return likelihoodValues;
	}

	/**
	 * @param likelihoodValues
	 *            the likelihoodValues to set
	 */
	public void setLikelihoodValues(double[] likelihoodValues) {
		this.likelihoodValues = likelihoodValues;
	}

	/**
	 * @return the headersInformation
	 */
	public Map<String, Integer> getHeadersInformation() {
		return headersInformation;
	}

	/**
	 * @param headersInformation
	 *            the headersInformation to set
	 */
	public void setHeadersInformation(Map<String, Integer> headersInformation) {
		this.headersInformation = headersInformation;
	}

	/**
	 * @return the data
	 */
	public JavaRDD<BayesianPropertiesDTO> getData() {
		return data;
	}

	/**
	 * @param data
	 *            the data to set
	 */
	public void setData(JavaRDD<BayesianPropertiesDTO> data) {
		this.data = data;
	}

}
