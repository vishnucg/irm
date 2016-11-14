package com.toyota.analytix.common.util;

import java.util.Arrays;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.toyota.analytix.common.exceptions.AnalytixRuntimeException;

/**
 * Class to handle all the property values from Query.properties
 * 
 * Currently Query.properties holds 3 types of properties. 1. Properties that
 * drive the code 2. Properties that give the name of the parameters, that need
 * to looked up into a parameter table to get a value 3. Error Messages.
 * 
 * Currently, we have explicit methods where it makes sense. We can use
 * QueryProperties.getValue for any property that is not accessed through the
 * explicit methods 
 * TODO combine this and MessageProperties
 */
public final class QueryProperties2 {

	private static Logger logger = Logger.getLogger(QueryProperties2.class);
	
	private QueryProperties2() {
	}

	private static PropertiesConfiguration constantproperties;
	private static double[] percentileBuckets = null;
	private static double[] salesShareExclusions = null;
	private static double[] MSRPRatioExclusions = null;


	private static final String PERCENTILE_BUCKETS = "doubles";
	private static final String SALES_SHARE_EXCLUSIONS = "share_msrp_excl";
	private static final String MSRP_RATIO_EXCLUSIONS = "msrp_ratio_excl";

	public static double[] getPercentileBuckets() {
		if (percentileBuckets != null)
			return percentileBuckets;

		synchronized (QueryProperties2.class) {
			String[] doublesProperty = constantproperties.getString(PERCENTILE_BUCKETS).split(",");
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
		
		String[] doublesProperty = StringUtils.split(constantproperties.getString(SALES_SHARE_EXCLUSIONS), ",");
		salesShareExclusions = new double[doublesProperty.length];
		for (int i = 0; i < doublesProperty.length; i++) {
			if(doublesProperty[i] != null)
				salesShareExclusions[i] = Double.parseDouble(doublesProperty[i]);
		}
		logger.info("Sales Share Exlcusions:" + Arrays.toString(salesShareExclusions));
		return salesShareExclusions;
	}

	public static double[] getMSRPRatioExclusions() {
		if (MSRPRatioExclusions != null)
			return MSRPRatioExclusions;
		String[] doublesProperty = StringUtils.split(constantproperties.getString(MSRP_RATIO_EXCLUSIONS),",");
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
		return constantproperties.getString(key);
	}

	public static void create(String propertiesFile) {
		try {
			constantproperties = new PropertiesConfiguration(propertiesFile);
		} catch (ConfigurationException e1) {
			logger.error("Query Properties could not be loaded..");
			throw new AnalytixRuntimeException(e1);
		}
	}

}
