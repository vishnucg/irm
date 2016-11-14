package com.toyota.analytix.common.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;
import com.toyota.analytix.mrm.msrp.dataPrep.DataLoader;

/**
 * Class to handle all the property values from Query.properties
 * 
 * Currently Query.properties holds 3 types of properties. 1. Properties that
 * drive the code 2. Properties that give the name of the parameters, that need
 * to looked up into a parameter table to get a value 3. Error Messages.
 * 
 * Currently, we have explicit methods where it makes sense. We can use
 * QueryProperties.getValue for any property that is not accessed through the
 * explicit methods TODO combine this and MessageProperties
 */
public final class QueryProperties {

	private static Logger logger = Logger.getLogger(QueryProperties.class);
	public static String QUERY_PROPS = "query_properties";
	
	// private static QueryProperties q;
	private static Properties queryProperties;
	private static Properties constantproperties;

	private static double[] percentileBuckets = null;
	private static double[] salesShareExclusions = null;
	private static double[] MSRPRatioExclusions = null;

	static {
		constantproperties = new Properties();
		try {
			constantproperties.load(QueryProperties.class.getResourceAsStream("/props.properties"));
		} catch (IOException e1) {
			throw new AnalytixRuntimeException(e1);
		}
	}

	public static void create(String file, Properties props) {
		queryProperties = new Properties();
		try {
			queryProperties.load(new FileInputStream(file));
		} catch (IOException e1) {
			throw new AnalytixRuntimeException(e1);
		}
		if (props != null) {
			// overwrite props
			Enumeration<?> e = props.propertyNames();

			while (e.hasMoreElements()) {
				String key = (String) e.nextElement();
				queryProperties.setProperty(key.toString(), props.getProperty(key));
			}
		}
	}

	private static final String PERCENTILE_BUCKETS = "doubles";
	private static final String SALES_SHARE_EXCLUSIONS = "share_msrp_excl";
	private static final String MSRP_RATIO_EXCLUSIONS = "msrp_ratio_excl";

	public static double[] getPercentileBuckets() {
		if (percentileBuckets != null)
			return percentileBuckets;

		synchronized (QueryProperties.class) {
			String[] doublesProperty = constantproperties.getProperty(PERCENTILE_BUCKETS).split(",");
			percentileBuckets = new double[doublesProperty.length * 2];
			for (int i = 0; i < doublesProperty.length; i++) {
				percentileBuckets[i] = Double.parseDouble(doublesProperty[i]);
				percentileBuckets[i + doublesProperty.length] = 1 - percentileBuckets[i];
			}
			// sort the arrays list
			Arrays.sort(percentileBuckets);
		}

		return percentileBuckets;
	}

	public static double[] getSalesShareExclusions() {
		if (salesShareExclusions != null)
			return salesShareExclusions;

		String[] doublesProperty = StringUtils.split(constantproperties.getProperty(SALES_SHARE_EXCLUSIONS), ",");
		salesShareExclusions = new double[doublesProperty.length];
		for (int i = 0; i < doublesProperty.length; i++) {
			if (doublesProperty[i] != null)
				salesShareExclusions[i] = Double.parseDouble(doublesProperty[i]);
		}
		logger.info("Sales Share Exlcusions:" + Arrays.toString(salesShareExclusions));
		return salesShareExclusions;
	}

	public static double[] getMSRPRatioExclusions() {
		if (MSRPRatioExclusions != null)
			return MSRPRatioExclusions;
		String[] doublesProperty = StringUtils.split(constantproperties.getProperty(MSRP_RATIO_EXCLUSIONS), ",");
		MSRPRatioExclusions = new double[doublesProperty.length];
		for (int i = 0; i < doublesProperty.length; i++) {
			MSRPRatioExclusions[i] = Double.parseDouble(doublesProperty[i].trim());
		}
		logger.info("MSRP Ratio Exlcusions:" + Arrays.toString(MSRPRatioExclusions));

		return MSRPRatioExclusions;
	}

	/**
	 * Generic method to return value of a key from Query properties
	 * 
	 * @param key
	 * @return
	 */
	public static String getValue(String key) {
		if (key == null) {
			logger.debug("key is null");
		}
		if (constantproperties.getProperty(key) == null) {
			logger.debug("constantproperties.getProperty(key) is null");
		}
		return constantproperties.getProperty(key);
	}

	public static String getQueryValue(String key) {
		if (queryProperties == null) {
			logger.debug("Constant Properties is null??");
			create(DataLoader.queryPropPath, null);
		}
		if (key == null) {
			logger.debug("key is null");
		}
		if (queryProperties.getProperty(key) == null) {
			logger.debug("queryProperties.getProperty(key) is null");
		}
		return queryProperties.getProperty(key);
	}
	
	public static void main(String[] args) {
		QueryProperties.create("/Users/Subu/Desktop/Work/KaizenAnalytix/MSRPElasticityModel/Query.properties", null);
		System.out.println(MRMUtil.getValue(MessageConstants.RESIDUAL));
		System.out.println(MRMUtil.getValue(MessageConstants.DEPENDENT_VAR));
		System.out.println("skip-driver-table-population:" + QueryProperties.getQueryValue("skip-driver-table-population"));
		String populateData = QueryProperties.getQueryValue("skip-driver-table-population");
		System.out.println("skip-driver-table-population:" + (StringUtils.isNotBlank(populateData) && populateData.equalsIgnoreCase("true")));
	}

}
