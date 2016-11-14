
package com.toyota.analytix.common.spark;

import java.io.PrintStream;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import com.toyota.analytix.common.util.Constants;

/**
 * A super class for initializing and storing contexts, property files etc
 *
 */
public abstract class SparkRunnableModel {

	protected final Logger logger = Logger.getLogger(getClass());
	private static final Logger sysoutLogger = Logger
			.getLogger(SparkRunnableModel.class.getCanonicalName() + ".CustomSysoutLogger");

	public static JavaSparkContext sc = null;
	public static HiveContext hiveContext = null;
	public static SQLContext sqlContext = null;

	protected SparkRunnableModel(String appName) {
		String clientName = System.getProperty("spark.master");
		if (StringUtils.isBlank(clientName)){
			clientName = Constants.HIVE_CLIENT_NAME;
		}
		// Getting a java spark context object by using the constants
		sc = new JavaSparkContext(clientName, Constants.HIVE_APP_NAME);

		// Creating a hive context object connection by using java spark
		hiveContext = new org.apache.spark.sql.hive.HiveContext(JavaSparkContext.toSparkContext(sc));
		// sql context
		sqlContext = new SQLContext(sc);
		
		// All sysouts are routed to logger. Useful for dataframe.show
		System.setOut(createLoggingProxy(System.out));
	}
	
	public abstract void processModel(Properties properties) throws Exception;

	// Logging proxy for routing sysouts
	//\n is to make sure the sysout gets printed in a separate line
	public static PrintStream createLoggingProxy(final PrintStream realPrintStream) {
		return new PrintStream(realPrintStream) {
			public void print(final String string) {
				realPrintStream.print(string);
				sysoutLogger.info("\n"+string);
			}
			public void println(final String string) {
				realPrintStream.println(string);
				sysoutLogger.info("\n"+string);
			}
	    };
	    }
}
