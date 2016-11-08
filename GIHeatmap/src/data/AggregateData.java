package data;

import org.apache.log4j.Logger;

public class  AggregateData {
	public String seriesFuelName;
	public int businessMonth;
	public int nonFleetRetailSalesSum;
	public double seasonalFactorSum;
	public int maxBusinessMonth;
	public int fleetSalesSum;
	public int historicalSalesSum;
	
	private static final Logger logger = Logger.getLogger(AggregateData.class);
	
	public AggregateData(String seriesFuelName, int businessMonth) {
		this.seriesFuelName = seriesFuelName;
		this.businessMonth = businessMonth;
		this.nonFleetRetailSalesSum = 0;
		this.seasonalFactorSum = 0.0;
		this.fleetSalesSum = 0;
		this.historicalSalesSum = 0;
	}

	public int getHistoricalSalesSum() {
		return historicalSalesSum;
	}

	public void setHistoricalSalesSum(int historicalSalesSum) {
		this.historicalSalesSum = historicalSalesSum;
	}

	public String getSeriesFuelName() {
		return seriesFuelName;
	}

	public int getBusinessMonth() {
		return businessMonth;
	}

	public int getNonFleetRetailSalesSum() {
		return nonFleetRetailSalesSum;
	}

	public void setNonFleetRetailSalesSum(int nonFleetRetailSalesSum) {
		this.nonFleetRetailSalesSum = nonFleetRetailSalesSum;
	}

	public double getSeasonalFactorSum() {
		return Math.round(seasonalFactorSum);
	}

	public void setSeasonalFactorSum(int seasonalFactorSum) {
		this.seasonalFactorSum = seasonalFactorSum;
	}
	
	public void addNonFleetRetailSales(int nonFleetRetailSales) {
//		logger.debug(this);
//		logger.debug("Adding sales: " + nonFleetRetailSales);
		this.nonFleetRetailSalesSum += nonFleetRetailSales;
	}
	
	public void addSeasonalFactor(double seasonalFactor) {
		this.seasonalFactorSum += seasonalFactor;
	}
	
	public void addFleetSales(int fleetSales) {
		this.fleetSalesSum += fleetSales;
	}
	
	public void addHistoricalSales(int historicalSales) {
		this.historicalSalesSum += historicalSales;
	}
	
	public void setMaxBusinessMonth(int businessMonth) {
		this.maxBusinessMonth = businessMonth;
	}
	
	public int getMaxBusinessMonth() {
		return this.maxBusinessMonth;
	}

	public int getFleetSalesSum() {
		return fleetSalesSum;
	}

	public void setFleetSales(int fleetSales) {
		this.fleetSalesSum = fleetSales;
	}

	@Override
	public String toString() {
		return "AggregateData [seriesFuelName=" + seriesFuelName + ", businessMonth=" + businessMonth
				+ ", nonFleetRetailSalesSum=" + nonFleetRetailSalesSum + ", seasonalFactorSum=" + seasonalFactorSum
				+ ", maxBusinessMonth=" + maxBusinessMonth + ", fleetSalesSum=" + fleetSalesSum + "]";
	}
	
}
