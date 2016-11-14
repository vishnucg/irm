package com.toyota.analytix.mrm.msrp.dataPrep;

import java.sql.Timestamp;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.common.util.QueryProperties;


public abstract class DataLoader {
	
	private static final Logger logger = Logger.getLogger(DataLoader.class);
	private static DataLoader dataLoader;
	private static String DATALOADER = "dataloader";
	private static enum DataLoaderType{ HiveDataLoader, CSVDataLoader};
	
	//TODO UGLY
	public static String queryPropPath;
	
	
	protected Timestamp latestTimeStamp;
	private DataFrame seriesData;
	private DataFrame filterData;
	private DataFrame regionData;
	private DataFrame MSRPData;
	private DataFrame parameters;
	
	//Used to check if the data is loaded 
	private boolean isDataLoaded;
	
	//Methods that need to be implemented by child classes
	protected abstract Timestamp getLatestTimestamp();
	protected abstract DataFrame loadSeriesData();
	protected abstract DataFrame loadFilterData();
	protected abstract DataFrame loadRegionData();
	protected abstract DataFrame loadMSRPData();
	protected abstract DataFrame loadParameterMap();

	// TODO make this abstract
	public void saveOutput() {

	}

	/**
	 * Factory method to get the appropriateDataLoader
	 * 
	 * @return DataLoader
	 */
	private static DataLoader getDataLoader(Properties properties) {
		// Load QueryProperties
		String queryProperties = properties.getProperty(QueryProperties.QUERY_PROPS);
		if (StringUtils.isEmpty(queryProperties))
			throw new AnalytixRuntimeException("query_properties not defined in properties.");
		queryPropPath = queryProperties;
		QueryProperties.create(queryProperties, properties);

		if (dataLoader == null) {
			String dataLoaderType = properties.getProperty(DATALOADER);

			synchronized (DataLoader.class) {
				if (StringUtils.isNotBlank(dataLoaderType)
						&& dataLoaderType.equalsIgnoreCase(DataLoaderType.CSVDataLoader.toString()))
					dataLoader = new CSVDataLoader();
				else {
					// if dataloader not defined, use Hive
					dataLoader = new HiveDataLoader();
				}
			}
		}
		return dataLoader;
	}

	/**
	 * Loads data for series, region and MSRP.
	 * 
	 * @return Returns the DataLoader that has series data, region data and MSRP
	 *         Data
	 */
	public static DataLoader loadData(Properties properties) {
		DataLoader dataLoader = getDataLoader(properties);
		dataLoader.latestTimeStamp = dataLoader.getLatestTimestamp();
		dataLoader.seriesData = dataLoader.loadSeriesData();
		dataLoader.filterData = dataLoader.loadFilterData();
		
		dataLoader.seriesData.show();
		dataLoader.regionData = dataLoader.loadRegionData();
		dataLoader.MSRPData = dataLoader.loadMSRPData();
		
		DataFrame df = dataLoader.loadParameterMap();
		logger.debug("*************Loaded Parameters from Parameter Table**********");
		df.show();
		dataLoader.parameters = df;
		
			
		dataLoader.isDataLoaded = true;
		return dataLoader;
	}

	/**
	 * gets the loaded data
	 * 
	 * @throws IllegalStateException
	 *             Exception is thrown if DataLoder.loadData() is not called
	 *             before.x
	 * @return DataFrame
	 */
	public DataFrame getSeriesData() {
		if (!isDataLoaded)
			throw new IllegalStateException("Data is not loaded. Use DataLoader.loadData first.");
		return seriesData;
	}

	/**
	 * gets the loaded data
	 * 
	 * @throws IllegalStateException
	 *             Exception is thrown if DataLoder.loadData() is not called
	 *             before.x
	 * @return DataFrame
	 */
	public DataFrame getRegionData() {
		if (!isDataLoaded)
			throw new IllegalStateException("Data is not loaded. Use DataLoader.loadData first.");
		return regionData;
	}

	/**
	 * gets the loaded data
	 * 
	 * @throws IllegalStateException
	 *             Exception is thrown if DataLoder.loadData() is not called
	 *             before.x
	 * @return DataFrame
	 */
	public DataFrame getMSRPData() {
		if (!isDataLoaded)
			throw new IllegalStateException("Data is not loaded. Use DataLoader.loadData first.");
		return MSRPData;
	}

	/**
	 * gets the loaded data
	 * 
	 * @throws IllegalStateException
	 *             Exception is thrown if DataLoder.loadData() is not called
	 *             before.x
	 * @return DataFrame
	 */
	public DataFrame getFilterData() {
		if (!isDataLoaded)
			throw new IllegalStateException("Data is not loaded. Use DataLoader.loadData first.");
		return filterData;
	}

	/**
	 * gets the loaded data
	 * 
	 * @throws IllegalStateException
	 *             Exception is thrown if DataLoder.loadData() is not called
	 *             before.x
	 * @return DataFrame
	 */
	public Timestamp getTimestamp() {
		if (!isDataLoaded)
			throw new IllegalStateException("Data is not loaded. Use DataLoader.loadData first.");
		return latestTimeStamp;
	}
	/**
	 * gets the parameter map
	 * 
	 * @throws IllegalStateException
	 *             Exception is thrown if DataLoder.loadData() is not called
	 *             before.x
	 * @return Parameter Map
	 */
	public DataFrame getParameters() {
		if (!isDataLoaded)
			throw new IllegalStateException("Data is not loaded. Use DataLoader.loadData first.");
		return parameters;
	}
	
}
