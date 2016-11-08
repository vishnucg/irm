package model;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import data.HeatMapDataEntry;
import data.TargetVsPaceResult;
import util.NetworkCommunicator;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import data.DataCalculator;
import data.DataPreparation;

public class GIHeatmap implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5406322266834355577L;
	private static final Logger logger = Logger.getLogger(GIHeatmap.class);
	private static List<HeatMapDataEntry> hmdes;
	private static  ArrayList<TargetVsPaceResult> altvps;
	
	
	public static void main(String[] args) throws IOException {
		Properties queryproperties = new Properties();
		
		try {
			queryproperties.load(new FileReader("config.properties"));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

//		DataFrame inputdata = NetworkCommunicator.getHiveContext().sql(queryproperties.getProperty("test_query"));
		
		Boolean hiveMode = Boolean.valueOf(queryproperties.getProperty("HIVE_MODE"));
		
		if (hiveMode) {
			BasicConfigurator.configure();
			// pull in data from file/query
			String userid = null;
			String model = null;
			String database_name = null;
			String integrated_layer_database = null;
			String maxBusinessMonth = null;
			String propertiesFile = null;
			if(args.length==4) {
			 userid = args[0];
			 model = args[1];
			 database_name = args[2];
			 integrated_layer_database = args[3];
//			 propertiesFile = args[4];
			} else {
				System.out.println("Not enough arguments\n");
				System.exit(1);
			}
			
			String query = queryproperties.getProperty("max_date_query").replace("#database_name", database_name);
			
			logger.debug(query);
			maxBusinessMonth = String.valueOf(NetworkCommunicator.getHiveContext().sql(query).collect()[0].get(0));
			
			logger.debug("Max Business Month: " + maxBusinessMonth);
			System.out.println("Max Business Month: " + maxBusinessMonth);
//			System.exit(1);
			NetworkCommunicator.getHiveContext().sql("use "+ database_name);
			DataPreparation dp= new DataPreparation();
			
			hmdes = dp.populateHeatMapInputData(userid, model, integrated_layer_database);
			
			logger.warn("Heatmap data has been populated\n");
			
			// perform calculations on data
			DataCalculator dCalc = new DataCalculator();
			try{
				logger.warn("Calculating data\n");
				dCalc.CalculateData(hmdes, maxBusinessMonth);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			
			logger.warn("Process complete, model exiting\n");
		} else {
//			BasicConfigurator.configure();
//			DataPreparation dp = new DataPreparation();
//			// pull in data from file/query
//			hmdes = dp.populateHeatMapInputData();
////			dp.calculateRecentIncentivesMetricInput(hmdes);
//			
//			// perform calculations on data
//			DataCalculator dCalc = new DataCalculator();
//			dCalc.CalculateData(hmdes, "201604");
		}
	}

}

