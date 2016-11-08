package data;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.TreeMap;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import util.NetworkCommunicator;

public class DataCalculator implements Serializable  {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1602947781086039212L;

	private static final Logger logger = Logger.getLogger(DataCalculator.class);
	
	ArrayList<TargetVsPaceResult> altvps = null;
	ArrayList<DealerStockMetricResult> aldsmr = null;
	ArrayList<MSRPRigidityResult> almsrprr = null;
	ArrayList<RecentIncentivesMetricResult> alrimr = null;
	
	public DataCalculator() {
		altvps = new ArrayList<TargetVsPaceResult>();
		aldsmr = new ArrayList<DealerStockMetricResult>();
		almsrprr = new ArrayList<MSRPRigidityResult>();
		alrimr = new ArrayList<RecentIncentivesMetricResult>();
	}
	
	private static ResourceBundle resourceBundle = null;

	// Loading the query properties file.
	static {
		resourceBundle = ResourceBundle.getBundle("Config");
	}

	public void CalculateData(List<HeatMapDataEntry> hmdes, String maxBusinessMonth) {	
		DateUtils du;
		
		DealerStockMetricResult dsmr = null;
		MSRPRigidityResult msrprr = null;
		TargetVsPaceResult tvpr = null;
		RecentIncentivesMetricResult rimr = null;
		
		AggregateDataManager adm = new AggregateDataManager();
		String seriesFound = "";
		Integer modelYear = 0;
		
		logger.setLevel(Level.DEBUG);
		
		List<String> gioutput = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyyHHmmss");
		SimpleDateFormat ctsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat effdf = new SimpleDateFormat("yyyy-MM-dd");
		
		String timeStamp = sdf.format(new Date());
		
		int noOfDays = 14; //i.e two weeks
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());            
		calendar.add(Calendar.DAY_OF_YEAR, noOfDays);
		String effDate = effdf.format(calendar.getTime());
		String createTimestamp = ctsdf.format(new Date());
		
		int counter = 0;
		String tvprs = "NULL", dsmrs = "NULL", ridge = "NULL", rim = "NULL";
		
		
		
		for (HeatMapDataEntry hmde : hmdes) {
			if ((!hmde.MAKE.equals("Toyota") && !hmde.MAKE.equals("Lexus") && !hmde.MAKE.equals("Scion"))) {
				continue;
			}
			
			if (!seriesFound.contains(hmde.SERIES_FUEL_NM)) {
				seriesFound += hmde.SERIES_FUEL_NM + ",";
			}
			
			modelYear = Integer.parseInt(String.valueOf(hmde.BUSINESS_MONTH).substring(0, 4));
			
			if (hmde.NONFLEET_SALES > 0) {
				adm.addNonFleetSalesEntry(hmde.MAKE, hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, hmde.NONFLEET_SALES);
				adm.addNonFleetSalesEntry(hmde.MAKE, hmde.SERIES_FUEL_NM, modelYear, hmde.NONFLEET_SALES);
			}
			
			if (hmde.SEASONAL_FACTOR > 0.0) {
				adm.addSeasonalFactorEntry(hmde.MAKE, hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, hmde.SEASONAL_FACTOR);
				adm.addSeasonalFactorEntry(hmde.MAKE, hmde.SERIES_FUEL_NM, modelYear, hmde.SEASONAL_FACTOR);
			}
			
			if (hmde.FLEET_SALES > 0) {
				adm.addFleetSales(hmde.MAKE, hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, hmde.FLEET_SALES);
				adm.addFleetSales(hmde.MAKE, hmde.SERIES_FUEL_NM, modelYear, hmde.FLEET_SALES);
			}
			
			if (hmde.HISTORICAL_SALES > 0) {
				adm.addHistoricalSales(hmde.MAKE, hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, hmde.HISTORICAL_SALES);
				adm.addHistoricalSales("", hmde.SEGMENT, hmde.BUSINESS_MONTH, hmde.HISTORICAL_SALES);
				adm.addHistoricalSales(hmde.MAKE, hmde.SERIES_FUEL_NM, modelYear, hmde.HISTORICAL_SALES);
			}
		}

		for (HeatMapDataEntry hmde : hmdes) {
			if (!hmde.MAKE.equals("Toyota") && !hmde.MAKE.equals("Lexus") && !hmde.MAKE.equals("Scion") || hmde.BUSINESS_MONTH > Integer.parseInt(maxBusinessMonth)) {
				continue;
			}
			
			
			try{
				logger.info("Calculating Target vs Pace");
				//calculate target vs pace
				tvpr = calculateTargetVsPace(hmde.BUSINESS_MONTH, 3, hmde, adm);
				if (tvpr == null) {
					
					//tvpr.setTvp(0.0);
					logger.debug("TargetVsPace calculation issue: " + hmde);
				} else {
					tvprs = String.valueOf(tvpr.getTvp());
					logger.debug("TargetVsPace calculated: " + tvpr);
					altvps.add(tvpr);
				}
			}
			catch(Exception e)
			{
				logger.error("Problem calculating Target vs Pace "+e.getMessage());
			}
			
			// calculate dealer stock metric
			dsmr = calculateDealerStockMetric(hmde);
			if (dsmr == null) {
				//dsmr.setDealer_stock_metric(0);
				//System.out.println("DealerStockMetric calculation issue: " + hmde);
			} else {
				dsmrs = String.valueOf(dsmr.getDealerStockMetric());
				
				aldsmr.add(dsmr);
			}

			// calculate msrp rigidity
			msrprr = calculateMSRPRigidity(hmde);
			if (msrprr == null) {
//				msrprr.setRigidity(0);
//				logger.warn("MSRPRigidity calculation issue: " + hmde);
			} else {
				ridge = String.valueOf(msrprr.getRigidity());
				almsrprr.add(msrprr);
			}
			
			
			
			rimr = this.calculateRecentIncentivesMetric(hmde,  (ArrayList<HeatMapDataEntry>) hmdes);
			if (rimr == null) {
				//rimr.setRecentIncentivesMetric(0.0);
				System.out.println("Error calculating RecentIncentivesMetric for :" + hmde.MAKE + ", " + hmde.SERIES_FUEL_NM + ", " + hmde.BUSINESS_MONTH);
			} else {
				rim = String.valueOf(rimr.getRecentIncentivesMetric());
				System.out.println("Calculating RecentIncentivesMetric for :" + hmde.MAKE + ", " + hmde.SERIES_FUEL_NM + ", " + hmde.BUSINESS_MONTH);
				System.out.println(rimr);
				alrimr.add(rimr);
			}
			
			
			// 
			
			try {
				if (tvpr != null && !String.valueOf(tvpr.getTvp()).equals("NaN") && !String.valueOf(tvpr.getTvp()).equals("Infinity")) {
					
					gioutput.add("IRM_PM_GI_HEATMAP@" + timeStamp + "#" + counter + "*53*" + hmde.MAKE + "*" + hmde.SERIES_FUEL_NM + "*99*" + hmde.BUSINESS_MONTH + "*Target vs Pace*" + ((int)(tvpr.getTvp() * 100)) + "*" + effDate + "*" + createTimestamp);
					counter += 1;
				}
			} catch(Exception e){
			    e.printStackTrace();
				continue;
			}
			
			try {		
				if (dsmr != null && !String.valueOf(dsmr.getDealerStockMetric()).equals("NaN") && !String.valueOf(dsmr.getDealerStockMetric()).equals("Infinity")) {
					gioutput.add("IRM_PM_GI_HEATMAP@" + timeStamp + "#" + counter + "*53*" + hmde.MAKE + "*" + hmde.SERIES_FUEL_NM + "*99*" + hmde.BUSINESS_MONTH + "*Dealer Stock*" + ((int)(dsmr.getDealerStockMetric() * 100)) + "*" + effDate + "*" + createTimestamp);
					counter += 1;
				}
			} catch(Exception e){
			    e.printStackTrace();
				continue;
			}
			
			try {			
				if (msrprr != null && !String.valueOf(msrprr.getRigidity()).equals("NaN") && !String.valueOf(msrprr.getRigidity()).equals("Infinity")) {
					gioutput.add("IRM_PM_GI_HEATMAP@" + timeStamp + "#" + counter + "*53*" + hmde.MAKE + "*" + hmde.SERIES_FUEL_NM + "*99*" + hmde.BUSINESS_MONTH + "*MSRP Rigidity*" + ((int)(msrprr.getRigidity() * 100)) + "*" + effDate + "*" + createTimestamp);
					counter += 1;
				}
				
			} catch(Exception e){
			    e.printStackTrace();
				continue;
			}
			
			try {
				if (rimr != null && !String.valueOf(rimr.getRecentIncentivesMetric()).equals("NaN")  && !String.valueOf(rimr.getRecentIncentivesMetric()).equals("Infinity")) {
					gioutput.add("IRM_PM_GI_HEATMAP@" + timeStamp + "#" + counter + "*53*" + hmde.MAKE + "*" + hmde.SERIES_FUEL_NM + "*99*" + hmde.BUSINESS_MONTH + "*Recent Incentives*" + ((int)(rimr.getRecentIncentivesMetric() * 100)) + "*" + effDate + "*" + createTimestamp);
					counter += 1;
				}
				
			} catch(Exception e){
			    e.printStackTrace();
				continue;
			}	
			
            
		}
		
		JavaRDD<String> girdd = NetworkCommunicator.getJavaSparkContext().parallelize(gioutput);

		try {
			
			JavaRDD<Row> dictrdd = girdd.map(new Function<String, Row>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = -7114717641270412314L;

				public Row call(String s) {
					String[] array = s.split("\\*");
					return RowFactory.create((Object[])array);
				}
			});

			 StructType newSchema = DataTypes
						.createStructType(new StructField[] {
								DataTypes.createStructField("RAW_DATA_SET_ROW_ID", DataTypes.StringType, false),
								DataTypes.createStructField("DATA_SET_ID", DataTypes.StringType, false),
								DataTypes.createStructField("MAKE_NM", DataTypes.StringType, false),
								DataTypes.createStructField("SERIES_FUEL_NM", DataTypes.StringType, false),
								DataTypes.createStructField("REGION_CD", DataTypes.StringType, false),
								DataTypes.createStructField("BUSINESS_MONTH", DataTypes.StringType, false),
								DataTypes.createStructField("BUSINESS_METRIC", DataTypes.StringType, false),
								DataTypes.createStructField("SCORE", DataTypes.StringType, false),
								DataTypes.createStructField("EFFECTIVE_DT", DataTypes.StringType, false),
								DataTypes.createStructField("CREATE_TS", DataTypes.StringType, false)});
					 
			DataFrame datagi = NetworkCommunicator.getHiveContext().createDataFrame(dictrdd,newSchema );

			datagi.insertInto("RAW_IRM_PM_GI_HEATMAP");
			
		} catch (Exception e) {
			logger.warn("Failure writing data to table\n");
			e.printStackTrace();
		}
	}
	
	public TargetVsPaceResult calculateTargetVsPace(int startMonth, int numMonths, HeatMapDataEntry hmde, AggregateDataManager adm) {
		double pace = this.calculatePace(hmde.MAKE, hmde.SERIES_FUEL_NM, startMonth, numMonths, adm);
		double tvp;
		TargetVsPaceResult tvpr = null;
		logger.debug("Target: " + hmde.SALES_TARGETS);
		logger.debug("Pace:" + pace);
		logger.debug("Fleet: " + adm.getFleetSales(hmde.MAKE, hmde.SERIES_FUEL_NM, Integer.parseInt(String.valueOf(startMonth).substring(0, 4))));
		
			
		if (pace != 0 && hmde.SALES_TARGETS != -1 && hmde.FLEET_SALES != -1) {
			tvp = hmde.SALES_TARGETS / (pace + hmde.FLEET_SALES);
			tvpr = new TargetVsPaceResult(hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, hmde.SALES_TARGETS * 12 / (pace + adm.getFleetSales(hmde.MAKE, hmde.SERIES_FUEL_NM, Integer.parseInt(String.valueOf(startMonth).substring(0, 4)))));
//			System.out.println(tvpr);
		} else {
//			System.out.println("Skipping TVP for: " + hmde);
		}
		
		return tvpr;
	}
	
	public double calculatePace(String make, String series, int startMonth, int numMonths, AggregateDataManager adm) {
		double pace = 0;
		String calendarYear = String.valueOf(startMonth).substring(0, 4);
		logger.debug("Calculating Pace for " + startMonth);
		String curMonth, curCYMonth;
		
		Date referenceDate = null;
		try {
			referenceDate = new SimpleDateFormat("yyyyMM").parse(String.valueOf(startMonth));
			logger.debug(referenceDate);
			
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		for (int i=0; i<numMonths; i++) {
			curMonth = new SimpleDateFormat("yyyyMM").format(DateUtils.addMonths(referenceDate,  (-1 * i)));
			// Verify current month data exists - TODO Loop
			if (!adm.algg.containsKey(make +  "|" + series + "|" + (curMonth))) {
				logger.error("Aggregate Data not found for " + series + " - " + (curMonth) );
			} else {
				logger.info("Aggregate Data found for " + series + " - " + (curMonth) + ": " + adm.algg.get(series + "|" + (curMonth)));
			}
		}
		
		// Verify annual data exists
		if (!adm.algg.containsKey(make +  "|" + series + "|" + calendarYear)) {
			logger.error("Aggregate Data not found for " + series + " - " + calendarYear );
		} else {
			logger.info("Aggregate Data found for " + series + " - " + calendarYear + ": " + adm.algg.get(make +  "|" + series + "|" + calendarYear));
		}
		
		Boolean historyExists = true;
		int nonFleetRetailSalesSum = 0;
		double seasonalFactorSum = 0.0;
		
		// verify that the number of historical months requested exist
		for (int i=0; i<numMonths; i++) {
			curMonth = new SimpleDateFormat("yyyyMM").format(DateUtils.addMonths(referenceDate,  (-1 * i)));
			curCYMonth = String.valueOf(startMonth).substring(0, 4) + new SimpleDateFormat("MM").format(DateUtils.addMonths(referenceDate,  (-1 * i)));
			if (!adm.algg.containsKey(make +  "|" + series + "|" + curMonth)) {
				historyExists = false;
				
			} else {
				logger.debug("NonFleetRetailSalesSum for " + curMonth +  ": " + adm.algg.get(make +  "|" + series + "|" + curMonth).nonFleetRetailSalesSum);
				logger.debug("SeasonalFactorSum for " + curMonth +  ": " + adm.algg.get(make +  "|" + series + "|" + curCYMonth).seasonalFactorSum);
				
				nonFleetRetailSalesSum += adm.algg.get(make +  "|" + series + "|" + curMonth).nonFleetRetailSalesSum;
				seasonalFactorSum += adm.algg.get(make +  "|" + series + "|" + curCYMonth).seasonalFactorSum;
			}
		}
		
		// verify that value exists for model year
		if (!adm.algg.containsKey(make +  "|" + series + "|" + calendarYear)) {
			historyExists = false;
		}
		
		if (historyExists) {
			logger.debug("PACE CALCULATION VALUES");
			logger.debug("NonFleetRetailSalesSum: " + nonFleetRetailSalesSum);
			logger.debug("SeasonalFactorSum: " + seasonalFactorSum);
			logger.debug("SeasonalFactorCalendarYear: " + adm.algg.get(make +  "|" + series + "|" + calendarYear).seasonalFactorSum);
			
			pace = (nonFleetRetailSalesSum) / (seasonalFactorSum) * adm.algg.get(make +  "|" + series + "|" + calendarYear).seasonalFactorSum;

		} else {
			logger.error("Pace Required historical inputs are not provided");
		}
		
		return pace;
		
		
	}

	public DealerStockMetricResult calculateDealerStockMetric(HeatMapDataEntry hmde) {
		int monthlySeriesDealerStock = hmde.DEALER_STOCK;
		double seriesMaxDealerStock = hmde.MAX_DEALER_STOCK;
		double recentDealerStock;
		DealerStockMetricResult dsmr = null;
		
		if (seriesMaxDealerStock == 0 || seriesMaxDealerStock == -1 || monthlySeriesDealerStock == -1) {
//			System.out.println("Dealer stock input variables incorrect..");
//			System.out.println(hmde);
			
		} else {
			recentDealerStock = monthlySeriesDealerStock / seriesMaxDealerStock;
			dsmr = new DealerStockMetricResult(hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, recentDealerStock);
//			System.out.println(dsmr.toString());
		}
		
		return dsmr;
	}
	
	public MSRPRigidityResult calculateMSRPRigidity(HeatMapDataEntry hmde) {
		int tunableConstant;
		double msrpElasticity;
		double msrpRigidity;
		MSRPRigidityResult msrprr = null;
		
		tunableConstant = Integer.parseInt(resourceBundle.getString("MSRP_RIGIDTIY_TUNABLE_CONSTANT"));
		
		msrpElasticity = hmde.ELASTICITY;
		
		if (msrpElasticity >= 0) {
//			System.out.println("Elasticity is >= 0..");
//			System.out.println(hmde);

		} else if (msrpElasticity == -1) {
//			System.out.println("Elasticity input is not correct");
//			System.out.println(hmde);
		} else {
			msrpRigidity = tunableConstant * Math.exp(msrpElasticity);
			msrprr = new MSRPRigidityResult(hmde.SERIES_FUEL_NM, hmde.BUSINESS_MONTH, msrpRigidity);
			
//			System.out.println(msrprr);
		}
		
		return msrprr;
	}

	public RecentIncentivesMetricResult calculateRecentIncentivesMetric(HeatMapDataEntry hmde, ArrayList<HeatMapDataEntry> hmdes) {
		//new RecentIncentivesMetricResult();
		RecentIncentivesMetricResult rimr = null;
		
		
		if (hmde.CFTP > 0 && hmde.INCENTIVES >= 0 && hmde.HISTORICAL_SALES > 0) {
			rimr = new RecentIncentivesMetricResult(hmde.MAKE, hmde.SERIES_FUEL_NM, hmde.SEGMENT, hmde.BUSINESS_MONTH, hmde.CFTP, hmde.INCENTIVES);
			logger.debug("Calculating Series Average CFTP Ratio");
			rimr.calculateSeriesAvgInctvPct(hmdes);
			logger.debug("Calculating Segment Average CFTP Ratio");
			rimr.calculateSegmentAvgCFTPRatio(hmdes);
			logger.debug("Calculating Segment Monthly CFTP Ratio");
			rimr.calculateSegmentMonthlyInctvPct(hmdes);
			logger.debug("Calculating Recent Incentives Metric");
			rimr.calculateRecentIncentivesMetric();
		}
		
		return rimr;
	}
	
	public void CalculateInputData() {
		
	}
}
