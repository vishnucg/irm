package data;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class AggregateDataManager implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(DataCalculator.class);
	
	HashMap<String, AggregateData> algg;
	
	public AggregateDataManager() {
		algg = new HashMap<String, AggregateData>();
	}
	
	public void addNonFleetSalesEntry(String makeName, String seriesFuelName, int businessMonth, int nonFleetRetailSales) {
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).addNonFleetRetailSales(nonFleetRetailSales);
		} else {
			AggregateData ad = new AggregateData(makeName + "|" + seriesFuelName, businessMonth);
			ad.addNonFleetRetailSales(nonFleetRetailSales);
			algg.put(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth), ad);
		}
	}
	
	public void addSeasonalFactorEntry(String makeName, String seriesFuelName, int businessMonth, double seasonalFactor) {
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).addSeasonalFactor(seasonalFactor);
		} else {
			AggregateData ad = new AggregateData(makeName + "|" + seriesFuelName, businessMonth);
			ad.addSeasonalFactor(seasonalFactor);
			algg.put(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth), ad);
		}
	}
	
	public void addFleetSales(String makeName, String seriesFuelName, int businessMonth, int fleetSales) {		
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).addFleetSales(fleetSales);
			
		} else {
			AggregateData agg = new AggregateData(makeName + "|" + seriesFuelName, businessMonth);
			agg.addFleetSales(fleetSales);
			algg.put(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth), agg);
		}
	}
	
	// ADD HISTORICAL SALES
	public void addHistoricalSales(String makeName, String seriesFuelName, int businessMonth, int historicalSales) {		
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).addHistoricalSales(historicalSales);
			
		} else {
			AggregateData agg = new AggregateData(makeName + "|" + seriesFuelName, businessMonth);
			agg.addHistoricalSales(historicalSales);
			algg.put(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth), agg);
		}
	}
	
	// GET HISTORICAL SALES
	public int getHistoricalSales(String makeName, String seriesFuelName, int businessMonth) { 
		int val = 0;
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			val = algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).getHistoricalSalesSum();
		} else {
			logger.warn("No HistoricalSales found for: " + makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth));
		}
		return val;
	}
	
	public int getNonFleetSales(String makeName, String seriesFuelName, int businessMonth) {
		int val = 0;
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			val = algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).getNonFleetRetailSalesSum();
		} else {
			logger.warn("No NonFleetSales found for: " + makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth));
		}
		return val;
	}
	
	public double getSeasonalFactor(String makeName, String seriesFuelName, int businessMonth) {
		double val = 0;
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			val = algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).getSeasonalFactorSum();
		} else {
			logger.warn("No SeasonalFactor found for: " + makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth));
		}
		return val;
	}
	
	public int getFleetSales(String makeName, String seriesFuelName, int businessMonth) { 
		int val = 0;
		if (algg.containsKey(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth))) {
			val = algg.get(makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth)).getFleetSalesSum();
		} else {
			logger.warn("No FleetSales found for: " + makeName + "|" + seriesFuelName + "|" + String.valueOf(businessMonth));
		}
		return val;
	}

}
