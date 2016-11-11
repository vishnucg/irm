package util;

/**
 * 
 */

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/*
 This class generates a connect object that is used to connect to hive database
 */

public class NetworkCommunicator {
	static final Logger mrmDevLogger = Logger
			.getLogger(NetworkCommunicator.class);

	// Variables HiveContext and JavaSparkContext are initialized.
	// Creates a JavaSparkContext that loads settings from system properties
	public static HiveContext hiveContext = null;
	private static JavaSparkContext sc = null;

	static {

		// Exception to catch lack of resource for the connection
		try {

			// Getting a java spark context object by using the constants
			sc = new JavaSparkContext(Constants.HIVE_CLIENT_NAME,
					Constants.HIVE_APP_NAME);
			// sc.addJar(SparkContext.jarOfClass(NetworkCommunicator.class).get());
			// Specifying a Jar Path
			sc.addJar("/irm/ihub/irmihubtxfer/inbound/misc/jars/application/mrm/IncentiveElasticity.jar");

			// sc.addJar("/home/irmprot/RVF_Java/IncentiveElasticity.jar");

			// Creating a hive context object connection by using java spark
			// context object
			hiveContext = new org.apache.spark.sql.hive.HiveContext(
					JavaSparkContext.toSparkContext(sc));
		} catch (Exception e) {
			e.printStackTrace();
			mrmDevLogger.warn("Failed to connect,Contact  ");
		}
	}

	// returning a connection object
	public static HiveContext getHiveContext() {
		return hiveContext;
	}

	// returning a java spark context object
	public static JavaSparkContext getJavaSparkContext() {
		return sc;
	}

	// Create a method to close the connection
	public static void closeJavaSparkContext() {
		sc.close();
	}
}
