/**
 * 
 */
package variableSelection;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This class contains the fields required for the variable selection module.
 * Here we have setters and getters for these fields. If we need any properties
 * from this , we can get using getters, if we want to set, we can set
 * additional properties for this class and can generate the setters and
 * getters.
 * 
 * @author Naresh
 *
 */
public class VariableSelectionPropertiesDTO implements Serializable {

	private static final long serialVersionUID = 845549570062462519L;

	private String dependantVar = null;
	private String inclusiveVar = null;
	private List<String> independentColumnList = null;
	private List<Integer> signList = null;
	private double vifCutoff = 0;
	private double pValue = 0.0;
	private List<Double> pvaluesList = null;
	private List<Double> coefficientList = null;
	private Map<String, Double> columnPvalues = null;
	private Map<String, Double> coeffList = null;
	private List<String> modifiedCoefList = null;
	private double coefMinValue;
	private Map<String, Double> modifiedCoefDetails = null;
	private List<String> vifValuesFinalList = null;
	private List<String[]> listOfValues = null;
	private double[][] x;
	private double[] y;
	private Map<String, Integer> headersInformation = null;
	private List<String> errorFreeList = null;
	private int noOfLines;
	Map<String, Double> vifValues = null;

	/**
	 * @return the dependantVar
	 */
	public String getDependantVar() {
		return dependantVar;
	}

	/**
	 * @param dependantVar
	 *            the dependantVar to set
	 */
	public void setDependantVar(String dependantVar) {
		this.dependantVar = dependantVar;
	}

	/**
	 * @return the inclusiveVar
	 */
	public String getInclusiveVar() {
		return inclusiveVar;
	}

	/**
	 * @param inclusiveVar
	 *            the inclusiveVar to set
	 */
	public void setInclusiveVar(String inclusiveVar) {
		this.inclusiveVar = inclusiveVar;
	}

	/**
	 * @return the independentColumnList
	 */
	public List<String> getIndependentColumnList() {
		return independentColumnList;
	}

	/**
	 * @param independentColumnList
	 *            the independentColumnList to set
	 */
	public void setIndependentColumnList(List<String> independentColumnList) {
		this.independentColumnList = independentColumnList;
	}

	/**
	 * @return the signList
	 */
	public List<Integer> getSignList() {
		return signList;
	}

	/**
	 * @param signList
	 *            the signList to set
	 */
	public void setSignList(List<Integer> signList) {
		this.signList = signList;
	}

	/**
	 * @return the vifCutoff
	 */
	public double getVifCutoff() {
		return vifCutoff;
	}

	/**
	 * @param vifCutoff
	 *            the vifCutoff to set
	 */
	public void setVifCutoff(double vifCutoff) {
		this.vifCutoff = vifCutoff;
	}

	/**
	 * @return the pValue
	 */
	public double getpValue() {
		return pValue;
	}

	/**
	 * @param pValue
	 *            the pValue to set
	 */
	public void setpValue(double pValue) {
		this.pValue = pValue;
	}

	/**
	 * @return the pvaluesList
	 */
	public List<Double> getPvaluesList() {
		return pvaluesList;
	}

	/**
	 * @param pvaluesList
	 *            the pvaluesList to set
	 */
	public void setPvaluesList(List<Double> pvaluesList) {
		this.pvaluesList = pvaluesList;
	}

	/**
	 * @return the coefficientList
	 */
	public List<Double> getCoefficientList() {
		return coefficientList;
	}

	/**
	 * @param coefficientList
	 *            the coefficientList to set
	 */
	public void setCoefficientList(List<Double> coefficientList) {
		this.coefficientList = coefficientList;
	}

	/**
	 * @return the columnPvalues
	 */
	public Map<String, Double> getColumnPvalues() {
		return columnPvalues;
	}

	/**
	 * @param columnPvalues
	 *            the columnPvalues to set
	 */
	public void setColumnPvalues(Map<String, Double> columnPvalues) {
		this.columnPvalues = columnPvalues;
	}

	/**
	 * @return the coeffList
	 */
	public Map<String, Double> getCoeffList() {
		return coeffList;
	}

	/**
	 * @param coeffList
	 *            the coeffList to set
	 */
	public void setCoeffList(Map<String, Double> coeffList) {
		this.coeffList = coeffList;
	}

	/**
	 * @return the modifiedCoefList
	 */
	public List<String> getModifiedCoefList() {
		return modifiedCoefList;
	}

	/**
	 * @param modifiedCoefList
	 *            the modifiedCoefList to set
	 */
	public void setModifiedCoefList(List<String> modifiedCoefList) {
		this.modifiedCoefList = modifiedCoefList;
	}

	/**
	 * @return the coefMinValue
	 */
	public double getCoefMinValue() {
		return coefMinValue;
	}

	/**
	 * @param coefMinValue
	 *            the coefMinValue to set
	 */
	public void setCoefMinValue(double coefMinValue) {
		this.coefMinValue = coefMinValue;
	}

	/**
	 * @return the modifiedCoefDetails
	 */
	public Map<String, Double> getModifiedCoefDetails() {
		return modifiedCoefDetails;
	}

	/**
	 * @param modifiedCoefDetails
	 *            the modifiedCoefDetails to set
	 */
	public void setModifiedCoefDetails(Map<String, Double> modifiedCoefDetails) {
		this.modifiedCoefDetails = modifiedCoefDetails;
	}

	/**
	 * @return the vifValuesFinalList
	 */
	public List<String> getVifValuesFinalList() {
		return vifValuesFinalList;
	}

	/**
	 * @param vifValuesFinalList
	 *            the vifValuesFinalList to set
	 */
	public void setVifValuesFinalList(List<String> vifValuesFinalList) {
		this.vifValuesFinalList = vifValuesFinalList;
	}

	/**
	 * @return the listOfValues
	 */
	public List<String[]> getListOfValues() {
		return listOfValues;
	}

	/**
	 * @param listOfValues
	 *            the listOfValues to set
	 */
	public void setListOfValues(List<String[]> listOfValues) {
		this.listOfValues = listOfValues;
	}

	/**
	 * @return the x
	 */
	public double[][] getX() {
		return x;
	}

	/**
	 * @param x
	 *            the x to set
	 */
	public void setX(double[][] x) {
		this.x = x;
	}

	/**
	 * @return the y
	 */
	public double[] getY() {
		return y;
	}

	/**
	 * @param y
	 *            the y to set
	 */
	public void setY(double[] y) {
		this.y = y;
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
	 * @return the errorFreeList
	 */
	public List<String> getErrorFreeList() {
		return errorFreeList;
	}

	/**
	 * @param errorFreeList
	 *            the errorFreeList to set
	 */
	public void setErrorFreeList(List<String> errorFreeList) {
		this.errorFreeList = errorFreeList;
	}

	/**
	 * @return the noOfLines
	 */
	public int getNoOfLines() {
		return noOfLines;
	}

	/**
	 * @param noOfLines
	 *            the noOfLines to set
	 */
	public void setNoOfLines(int noOfLines) {
		this.noOfLines = noOfLines;
	}

	/**
	 * @return the vifValues
	 */
	public Map<String, Double> getVifValues() {
		return vifValues;
	}

	/**
	 * @param vifValues
	 *            the vifValues to set
	 */
	public void setVifValues(Map<String, Double> vifValues) {
		this.vifValues = vifValues;
	}

}
