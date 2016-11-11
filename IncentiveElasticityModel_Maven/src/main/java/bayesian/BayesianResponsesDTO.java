/**
 * 
 */
package bayesian;

import java.io.Serializable;
import java.util.List;

/**
 * This class contains the fields required for the bayesian module while
 * generating the response from the module. Here we have setters and getters for
 * these fields. If we need any properties from this , we can get using getters,
 * if we want to set, we can set the properties using setters.
 * 
 * @author Naresh
 *
 */
public class BayesianResponsesDTO implements Serializable {

	private static final long serialVersionUID = 2192522938048177906L;

	private List<Double> centeredResidualValues;
	private List<Double> centeredAvgCompRatioValues;
	private List<Double> predValues;
	private double[] likelihoodValues;
	private float elastictyValue;

	/**
	 * @return the centeredResidualValues
	 */
	public List<Double> getCenteredResidualValues() {
		return centeredResidualValues;
	}

	/**
	 * @param centeredResidualValues
	 *            the centeredResidualValues to set
	 */
	public void setCenteredResidualValues(List<Double> centeredResidualValues) {
		this.centeredResidualValues = centeredResidualValues;
	}

	/**
	 * @return the centeredAvgCompRatioValues
	 */
	public List<Double> getCenteredAvgCompRatioValues() {
		return centeredAvgCompRatioValues;
	}

	/**
	 * @param centeredAvgCompRatioValues
	 *            the centeredAvgCompRatioValues to set
	 */
	public void setCenteredAvgCompRatioValues(
			List<Double> centeredAvgCompRatioValues) {
		this.centeredAvgCompRatioValues = centeredAvgCompRatioValues;
	}

	/**
	 * @return the predValues
	 */
	public List<Double> getPredValues() {
		return predValues;
	}

	/**
	 * @param predValues
	 *            the predValues to set
	 */
	public void setPredValues(List<Double> predValues) {
		this.predValues = predValues;
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
	 * @return the elastictyValue
	 */
	public float getElastictyValue() {
		return elastictyValue;
	}

	/**
	 * @param elastictyValue
	 *            the elastictyValue to set
	 */
	public void setElastictyValue(float elastictyValue) {
		this.elastictyValue = elastictyValue;
	}

}
