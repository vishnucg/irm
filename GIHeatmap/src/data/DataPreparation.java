package data;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.opencsv.CSVReader;

import util.NetworkCommunicator;




public class DataPreparation implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3552081976783117574L;
	private static final Logger logger = Logger.getLogger(DataPreparation.class);
	private ArrayList<HeatMapDataEntry> hmdes;
	
	public static List<String[]> parseCSVData(String filePath) {
		CSVReader reader = null;
		List<String[]> data = null;
		try {
			reader = new CSVReader(new FileReader(filePath));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if (reader != null) {
			try {
				data = reader.readAll();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return data.subList(1,  data.size() - 1);
	}
	
	public List<HeatMapDataEntry> populateHeatMapInputData(String userid, String model, String integratedLayer) throws IOException {
		Properties queryproperties = new Properties();
		queryproperties.load(DataPreparation.class.getResourceAsStream("/config.properties"));
		
		
//		NetworkCommunicator.getHiveContext().sql(queryproperties.getProperty("inputquery").replace("#database",database));
		List<String> series_list = new ArrayList<String>();
		List<Integer> business_month_list = new ArrayList<Integer>();
		
		DataFrame inputdata = NetworkCommunicator.getHiveContext().sql(queryproperties.getProperty("inputquery2").replace("#integrated_layer",integratedLayer));
		Timestamp timestamp = (Timestamp) NetworkCommunicator.getHiveContext()
			.sql(queryproperties.getProperty("timestampquery"))
			.filter("create_user_id = '" + userid
			+ "' and consumer_system_nm = '" + model + "'").first().get(0);
		
		try {
			DataFrame filterQueryDF = NetworkCommunicator.getHiveContext().sql(queryproperties.getProperty("filter_query")).filter(
					"create_ts = CAST('" + timestamp
					+ "' as TIMESTAMP) and create_user_id  = '" + userid
					+ "' and consumer_system_nm = '" + model + "'");
	
			
			for (Row Series : filterQueryDF.filter("filter_field='MAKE_NM,SERIES_NM,GRADE_NM'").collect()) {
				series_list.add(Series.get(1).toString().split(",")[1]);
			}
			for (Row business : filterQueryDF.collect()) {
	       		if (business.get(0).equals("BUSINESS_MONTH"))
	       			business_month_list.add((Integer) business.get(1));
	
				}
		} catch (Exception e) {
			logger.error(e);
			logger.error("Unable to find filters for timestamp: " + timestamp + ", create_user_id: " + userid + " consumer_system_nm:" + model);
		}
		
		if (series_list.isEmpty() || series_list.contains("ALL")) {
			series_list.clear();
			DataFrame series_table = NetworkCommunicator.getHiveContext().sql(queryproperties
					.getProperty("series_table"));
			for (Row series_value : series_table.collect()) {
				series_list.add(series_value.get(0).toString());
			}

		}
		
		
		
		
		if (business_month_list.isEmpty() || business_month_list.contains("ALL")) {
			business_month_list.clear();
			DataFrame series_table = NetworkCommunicator.getHiveContext().sql(queryproperties
					.getProperty("business_table"));
			for (Row series_value : series_table.collect()) {
				business_month_list.add(Integer.parseInt(series_value.getString(0)));
			}

		}
		
		
		System.out.println(business_month_list);
		
		inputdata.show();
		DataFrameManager datardd = new DataFrameManager();
		JavaRDD<HeatMapDataEntry> giInput = datardd.datatordd(inputdata);
		
		System.out.println("Count ******* "+giInput.count());
		
		
		return (ArrayList<HeatMapDataEntry>) giInput.collect();
		
	}
}
		
		
