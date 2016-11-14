package com.toyota.analytix.mrm.msrp.dataPrep;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import com.toyota.analytix.common.spark.SparkRunnableModel;


public class DataNationalizationManager extends SparkRunnableModel implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DataNationalizationManager.class);
//	ArrayList<MSRPInputEntry> mmies;
	DataFrame msrpInputDF = null;
	DataFrame msrpSumProd = null;
	
	public static void main(String[] args) {
		//System.setProperty("hadoop.home.dir", "C:\\Users\\135532\\Downloads\\hadoop-common-2.2.0-bin-master");
		new DataNationalizationManager("MSRP_MRM");
	}
	
	
	public DataNationalizationManager(String s) {
		super(s);
		
		BasicConfigurator.configure();
		// pull in data from file/query
		
		try {
			logger.debug("Creating Data Frame");
			// get data into a dataframe
			this.msrpInputDF = new DataNationalizationRunner().populateMSRPInputData();//.select("BUSINESS_MONTH", "VDW_SALES","BASE_MSRP","CFTP");
			DataNationalizationRunner.aggregateNationalData(msrpInputDF);
              
              // *********** TODO: Join all to create final input dataset
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while creating DataFrame");
			e.printStackTrace();
		}

	}


	

	@Override
	public void processModel(Properties properties) throws Exception {
		// TODO Auto-generated method stub
		
	}	
}
