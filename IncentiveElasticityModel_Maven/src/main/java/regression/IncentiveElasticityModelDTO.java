/**
 * 
 */
package regression;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * This class contains the fields which will set the information about the
 * Incentive elasticity model and get the information. This has setters and
 * getters to set and get the information.
 * 
 * @author Naresh
 *
 */
public class IncentiveElasticityModelDTO implements Serializable {

	private static final long serialVersionUID = 2019858203370035449L;
	private Timestamp start_time;
	private String model;
	private String databaseName;
	private String userID;
	private Timestamp timeStamp;
	private List<String> seriesList;
	private List<Integer> regionList;
	private List<String> regionValues;
	private Map<Integer, Integer> salesMap;
	private DataFrame inputTable;
	private Map<String, String> parameterMap;
	private DataFrame parameterTable;
	private DataFrame seriesTable;
	private DataFrame regionTable;
	private Double sumelasticity;
	private int elasticitycount;
	private Properties queryproperties;
	private HiveContext hiveContext;
	private DataFrame bestDataSet;
	private String series;
	private int region;
	private String incentive;
	private Properties constantproperties;
	private double[] doubles;
	private double[] doubles2;
	private DataFrame dataFrameOurlier;
	private Row[] rowspercent;
	private double[] percentile1;
	private double[] percentile2;
	private double[] shareInctvExcl;
	private double[] inctvRatioExcl;
	private JavaRDD<String> varSelectionInput;
	private Map<String, Double> variablemap;
	private List<String> independentList;
	private String independentString;
	private double[] residuals;
	private double pValue;
	private DataFrame dat;
	private double inctvCoef;
	private LinearRegressionWrapper simpleregression;
	private Map<DataFrame, String> bestDataSetMap;
	private double bestelasticity;
	private double bestpvalue;
	private boolean containsList;
	private DataFrame inctvbayesian;

	/**
	 * @return the model
	 */
	public String getModel() {
		return model;
	}

	/**
	 * @param model
	 *            the model to set
	 */
	public void setModel(String model) {
		this.model = model;
	}

	/**
	 * @return the databaseName
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * @param databaseName
	 *            the databaseName to set
	 */
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * @return the userID
	 */
	public String getUserID() {
		return userID;
	}

	/**
	 * @param userID
	 *            the userID to set
	 */
	public void setUserID(String userID) {
		this.userID = userID;
	}

	/**
	 * @return the timeStamp
	 */
	public Timestamp getTimeStamp() {
		return timeStamp;
	}

	/**
	 * @param timeStamp
	 *            the timeStamp to set
	 */
	public void setTimeStamp(Timestamp timeStamp) {
		this.timeStamp = timeStamp;
	}

	/**
	 * @return the seriesList
	 */
	public List<String> getSeriesList() {
		return seriesList;
	}

	/**
	 * @param seriesList
	 *            the seriesList to set
	 */
	public void setSeriesList(List<String> seriesList) {
		this.seriesList = seriesList;
	}

	/**
	 * @return the regionList
	 */
	public List<Integer> getRegionList() {
		return regionList;
	}

	/**
	 * @param regionList
	 *            the regionList to set
	 */
	public void setRegionList(List<Integer> regionList) {
		this.regionList = regionList;
	}

	/**
	 * @return the regionValues
	 */
	public List<String> getRegionValues() {
		return regionValues;
	}

	/**
	 * @param regionValues
	 *            the regionValues to set
	 */
	public void setRegionValues(List<String> regionValues) {
		this.regionValues = regionValues;
	}

	/**
	 * @return the salesMap
	 */
	public Map<Integer, Integer> getSalesMap() {
		return salesMap;
	}

	/**
	 * @param salesMap
	 *            the salesMap to set
	 */
	public void setSalesMap(Map<Integer, Integer> salesMap) {
		this.salesMap = salesMap;
	}

	/**
	 * @return the inputTable
	 */
	public DataFrame getInputTable() {
		return inputTable;
	}

	/**
	 * @param inputTable
	 *            the inputTable to set
	 */
	public void setInputTable(DataFrame inputTable) {
		this.inputTable = inputTable;
	}

	/**
	 * @return the parameterMap
	 */
	public Map<String, String> getParameterMap() {
		return parameterMap;
	}

	/**
	 * @param parameterMap
	 *            the parameterMap to set
	 */
	public void setParameterMap(Map<String, String> parameterMap) {
		this.parameterMap = parameterMap;
	}

	/**
	 * @return the parameterTable
	 */
	public DataFrame getParameterTable() {
		return parameterTable;
	}

	/**
	 * @param parameterTable
	 *            the parameterTable to set
	 */
	public void setParameterTable(DataFrame parameterTable) {
		this.parameterTable = parameterTable;
	}

	/**
	 * @return the seriesTable
	 */
	public DataFrame getSeriesTable() {
		return seriesTable;
	}

	/**
	 * @param seriesTable
	 *            the seriesTable to set
	 */
	public void setSeriesTable(DataFrame seriesTable) {
		this.seriesTable = seriesTable;
	}

	/**
	 * @return the regionTable
	 */
	public DataFrame getRegionTable() {
		return regionTable;
	}

	/**
	 * @param regionTable
	 *            the regionTable to set
	 */
	public void setRegionTable(DataFrame regionTable) {
		this.regionTable = regionTable;
	}

	/**
	 * @return the sumelasticity
	 */
	public Double getSumelasticity() {
		return sumelasticity;
	}

	/**
	 * @param sumelasticity
	 *            the sumelasticity to set
	 */
	public void setSumelasticity(Double sumelasticity) {
		this.sumelasticity = sumelasticity;
	}

	/**
	 * @return the elasticitycount
	 */
	public int getElasticitycount() {
		return elasticitycount;
	}

	/**
	 * @param elasticitycount
	 *            the elasticitycount to set
	 */
	public void setElasticitycount(int elasticitycount) {
		this.elasticitycount = elasticitycount;
	}

	/**
	 * @return the queryproperties
	 */
	public Properties getQueryproperties() {
		return queryproperties;
	}

	/**
	 * @param queryproperties
	 *            the queryproperties to set
	 */
	public void setQueryproperties(Properties queryproperties) {
		this.queryproperties = queryproperties;
	}

	/**
	 * @return the hiveContext
	 */
	public HiveContext getHiveContext() {
		return hiveContext;
	}

	/**
	 * @param hiveContext
	 *            the hiveContext to set
	 */
	public void setHiveContext(HiveContext hiveContext) {
		this.hiveContext = hiveContext;
	}

	/**
	 * @return the bestDataSet
	 */
	public DataFrame getBestDataSet() {
		return bestDataSet;
	}

	/**
	 * @param bestDataSet
	 *            the bestDataSet to set
	 */
	public void setBestDataSet(DataFrame bestDataSet) {
		this.bestDataSet = bestDataSet;
	}

	/**
	 * @return the series
	 */
	public String getSeries() {
		return series;
	}

	/**
	 * @param series
	 *            the series to set
	 */
	public void setSeries(String series) {
		this.series = series;
	}

	/**
	 * @return the region
	 */
	public int getRegion() {
		return region;
	}

	/**
	 * @param region
	 *            the region to set
	 */
	public void setRegion(int region) {
		this.region = region;
	}

	/**
	 * @return the incentive
	 */
	public String getIncentive() {
		return incentive;
	}

	/**
	 * @param incentive
	 *            the incentive to set
	 */
	public void setIncentive(String incentive) {
		this.incentive = incentive;
	}

	/**
	 * @return the constantproperties
	 */
	public Properties getConstantproperties() {
		return constantproperties;
	}

	/**
	 * @param constantproperties
	 *            the constantproperties to set
	 */
	public void setConstantproperties(Properties constantproperties) {
		this.constantproperties = constantproperties;
	}

	/**
	 * @return the doubles
	 */
	public double[] getDoubles() {
		return doubles;
	}

	/**
	 * @param doubles
	 *            the doubles to set
	 */
	public void setDoubles(double[] doubles) {
		this.doubles = doubles;
	}

	/**
	 * @return the doubles2
	 */
	public double[] getDoubles2() {
		return doubles2;
	}

	/**
	 * @param doubles2
	 *            the doubles2 to set
	 */
	public void setDoubles2(double[] doubles2) {
		this.doubles2 = doubles2;
	}

	/**
	 * @return the dataFrameOurlier
	 */
	public DataFrame getDataFrameOurlier() {
		return dataFrameOurlier;
	}

	/**
	 * @param dataFrameOurlier
	 *            the dataFrameOurlier to set
	 */
	public void setDataFrameOurlier(DataFrame dataFrameOurlier) {
		this.dataFrameOurlier = dataFrameOurlier;
	}

	/**
	 * @return the rowspercent
	 */
	public Row[] getRowspercent() {
		return rowspercent;
	}

	/**
	 * @param rowspercent
	 *            the rowspercent to set
	 */
	public void setRowspercent(Row[] rowspercent) {
		this.rowspercent = rowspercent;
	}

	/**
	 * @return the percentile1
	 */
	public double[] getPercentile1() {
		return percentile1;
	}

	/**
	 * @param percentile1
	 *            the percentile1 to set
	 */
	public void setPercentile1(double[] percentile1) {
		this.percentile1 = percentile1;
	}

	/**
	 * @return the percentile2
	 */
	public double[] getPercentile2() {
		return percentile2;
	}

	/**
	 * @param percentile2
	 *            the percentile2 to set
	 */
	public void setPercentile2(double[] percentile2) {
		this.percentile2 = percentile2;
	}

	/**
	 * @return the shareInctvExcl
	 */
	public double[] getShareInctvExcl() {
		return shareInctvExcl;
	}

	/**
	 * @param shareInctvExcl
	 *            the shareInctvExcl to set
	 */
	public void setShareInctvExcl(double[] shareInctvExcl) {
		this.shareInctvExcl = shareInctvExcl;
	}

	/**
	 * @return the inctvRatioExcl
	 */
	public double[] getInctvRatioExcl() {
		return inctvRatioExcl;
	}

	/**
	 * @param inctvRatioExcl
	 *            the inctvRatioExcl to set
	 */
	public void setInctvRatioExcl(double[] inctvRatioExcl) {
		this.inctvRatioExcl = inctvRatioExcl;
	}

	/**
	 * @return the varSelectionInput
	 */
	public JavaRDD<String> getVarSelectionInput() {
		return varSelectionInput;
	}

	/**
	 * @param varSelectionInput
	 *            the varSelectionInput to set
	 */
	public void setVarSelectionInput(JavaRDD<String> varSelectionInput) {
		this.varSelectionInput = varSelectionInput;
	}

	/**
	 * @return the variablemap
	 */
	public Map<String, Double> getVariablemap() {
		return variablemap;
	}

	/**
	 * @param variablemap
	 *            the variablemap to set
	 */
	public void setVariablemap(Map<String, Double> variablemap) {
		this.variablemap = variablemap;
	}

	/**
	 * @return the independentList
	 */
	public List<String> getIndependentList() {
		return independentList;
	}

	/**
	 * @param independentList
	 *            the independentList to set
	 */
	public void setIndependentList(List<String> independentList) {
		this.independentList = independentList;
	}

	/**
	 * @return the independentString
	 */
	public String getIndependentString() {
		return independentString;
	}

	/**
	 * @param independentString
	 *            the independentString to set
	 */
	public void setIndependentString(String independentString) {
		this.independentString = independentString;
	}

	/**
	 * @return the residuals
	 */
	public double[] getResiduals() {
		return residuals;
	}

	/**
	 * @param residuals
	 *            the residuals to set
	 */
	public void setResiduals(double[] residuals) {
		this.residuals = residuals;
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
	 * @return the dat
	 */
	public DataFrame getDat() {
		return dat;
	}

	/**
	 * @param dat
	 *            the dat to set
	 */
	public void setDat(DataFrame dat) {
		this.dat = dat;
	}

	/**
	 * @return the inctvCoef
	 */
	public double getInctvCoef() {
		return inctvCoef;
	}

	/**
	 * @param inctvCoef
	 *            the inctvCoef to set
	 */
	public void setInctvCoef(double inctvCoef) {
		this.inctvCoef = inctvCoef;
	}

	/**
	 * @return the simpleregression
	 */
	public LinearRegressionWrapper getSimpleregression() {
		return simpleregression;
	}

	/**
	 * @param simpleregression
	 *            the simpleregression to set
	 */
	public void setSimpleregression(LinearRegressionWrapper simpleregression) {
		this.simpleregression = simpleregression;
	}

	/**
	 * @return the bestDataSetMap
	 */
	public Map<DataFrame, String> getBestDataSetMap() {
		return bestDataSetMap;
	}

	/**
	 * @param bestDataSetMap
	 *            the bestDataSetMap to set
	 */
	public void setBestDataSetMap(Map<DataFrame, String> bestDataSetMap) {
		this.bestDataSetMap = bestDataSetMap;
	}

	/**
	 * @return the bestelasticity
	 */
	public double getBestelasticity() {
		return bestelasticity;
	}

	/**
	 * @param bestelasticity
	 *            the bestelasticity to set
	 */
	public void setBestelasticity(double bestelasticity) {
		this.bestelasticity = bestelasticity;
	}

	/**
	 * @return the bestpvalue
	 */
	public double getBestpvalue() {
		return bestpvalue;
	}

	/**
	 * @param bestpvalue
	 *            the bestpvalue to set
	 */
	public void setBestpvalue(double bestpvalue) {
		this.bestpvalue = bestpvalue;
	}

	/**
	 * @return the containsList
	 */
	public boolean isContainsList() {
		return containsList;
	}

	/**
	 * @param containsList
	 *            the containsList to set
	 */
	public void setContainsList(boolean containsList) {
		this.containsList = containsList;
	}

	public DataFrame getInctvbayesian() {
		return inctvbayesian;
	}

	public void setInctvbayesian(DataFrame inctvbayesian) {
		this.inctvbayesian = inctvbayesian;
	}

	public Timestamp getStart_time() {
		return start_time;
	}

	public void setStart_time(Timestamp start_time) {
		this.start_time = start_time;
	}

}
