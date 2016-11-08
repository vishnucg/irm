package data;

import java.io.Serializable;

public class HeatMapDataEntry implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6102652970319875356L;
	String MAKE;
	String SERIES_FUEL_NM;
	String SEGMENT;
	int BUSINESS_MONTH;
	int DEALER_STOCK;
	int SALES_TARGETS;
	double SEASONAL_FACTOR;
	int FLEET_SALES;
	int NONFLEET_SALES;
	int MAX_DEALER_STOCK;
	double CFTP;
	int HISTORICAL_SALES;
	double INCENTIVES;
	double ELASTICITY;
	String SEGMENT_CATEGORY_NM;
	
	public String getSEGMENT() {
		return SEGMENT;
	}

	public void setSEGMENT(String sEGMENT) {
		SEGMENT = sEGMENT;
	}

	public HeatMapDataEntry(String[] entry) {
		this.MAKE = entry[new HeatMapElementDecoder(HeatMapElement.MAKE).decodeIndex()].toUpperCase().equals("NULL") ? "-1" : entry[new HeatMapElementDecoder(HeatMapElement.MAKE).decodeIndex()].toString();
		this.SERIES_FUEL_NM = entry[new HeatMapElementDecoder(HeatMapElement.SERIES_FUEL_NM).decodeIndex()].toUpperCase().equals("NULL") ? "-1" : entry[new HeatMapElementDecoder(HeatMapElement.SERIES_FUEL_NM).decodeIndex()].toString();
		this.SEGMENT = entry[new HeatMapElementDecoder(HeatMapElement.SEGMENT).decodeIndex()].toUpperCase().equals("NULL") ? "-1" : entry[new HeatMapElementDecoder(HeatMapElement.SEGMENT).decodeIndex()].toString();
		this.SEGMENT_CATEGORY_NM = entry[new HeatMapElementDecoder(HeatMapElement.SEGMENT_CATEGORY_NM).decodeIndex()].toUpperCase().equals("NULL") ? "-1" : entry[new HeatMapElementDecoder(HeatMapElement.SEGMENT_CATEGORY_NM).decodeIndex()].toString();
		this.BUSINESS_MONTH = entry[new HeatMapElementDecoder(HeatMapElement.BUSINESS_MONTH).decodeIndex()].toUpperCase().equals("NULL") ? -1 : Integer.parseInt(entry[new HeatMapElementDecoder(HeatMapElement.BUSINESS_MONTH).decodeIndex()].toString());
		this.DEALER_STOCK = entry[new HeatMapElementDecoder(HeatMapElement.DEALER_STOCK).decodeIndex()].toUpperCase().equals("NULL") ? 0 : Integer.parseInt(entry[new HeatMapElementDecoder(HeatMapElement.DEALER_STOCK).decodeIndex()].toString());
		
		this.SALES_TARGETS = entry[new HeatMapElementDecoder(HeatMapElement.SALES_TARGETS).decodeIndex()].toUpperCase().equals("NULL") ? 0 : (int) Double.parseDouble(entry[new HeatMapElementDecoder(HeatMapElement.SALES_TARGETS).decodeIndex()].toString());
		
		this.SEASONAL_FACTOR = entry[new HeatMapElementDecoder(HeatMapElement.SEASONAL_FACTOR).decodeIndex()].toUpperCase().equals("NULL") ? -1 : Double.parseDouble(entry[new HeatMapElementDecoder(HeatMapElement.SEASONAL_FACTOR).decodeIndex()].toString());
		this.FLEET_SALES = entry[new HeatMapElementDecoder(HeatMapElement.FLEET_SALES).decodeIndex()].toUpperCase().equals("NULL") ? 0 : Integer.parseInt(entry[new HeatMapElementDecoder(HeatMapElement.FLEET_SALES).decodeIndex()].toString());
		this.NONFLEET_SALES = entry[new HeatMapElementDecoder(HeatMapElement.NONFLEET_SALES).decodeIndex()].toUpperCase().equals("NULL") ? 0 : Integer.parseInt(entry[new HeatMapElementDecoder(HeatMapElement.NONFLEET_SALES).decodeIndex()].toString());
		this.MAX_DEALER_STOCK = entry[new HeatMapElementDecoder(HeatMapElement.MAX_DEALER_STOCK).decodeIndex()].toUpperCase().equals("NULL") ? 0 : Integer.parseInt(entry[new HeatMapElementDecoder(HeatMapElement.MAX_DEALER_STOCK).decodeIndex()].toString());
		this.CFTP = entry[new HeatMapElementDecoder(HeatMapElement.CFTP).decodeIndex()].toUpperCase().equals("NULL") ? -1 : Double.parseDouble(entry[new HeatMapElementDecoder(HeatMapElement.CFTP).decodeIndex()].toString());
		this.HISTORICAL_SALES = entry[new HeatMapElementDecoder(HeatMapElement.HISTORICAL_SALES).decodeIndex()].toUpperCase().equals("NULL") ? 0 : Integer.parseInt(entry[new HeatMapElementDecoder(HeatMapElement.HISTORICAL_SALES).decodeIndex()].toString());
		this.INCENTIVES = entry[new HeatMapElementDecoder(HeatMapElement.INCENTIVES).decodeIndex()].toUpperCase().equals("NULL") ? 0 : Double.parseDouble(entry[new HeatMapElementDecoder(HeatMapElement.INCENTIVES).decodeIndex()].toString());
		this.ELASTICITY = entry[new HeatMapElementDecoder(HeatMapElement.ELASTICITY).decodeIndex()].toUpperCase().equals("NULL") ? -1 : Double.parseDouble(entry[new HeatMapElementDecoder(HeatMapElement.ELASTICITY).decodeIndex()].toString());
	}

	@Override
	public String toString() {
		return "HeatMapDataEntry [MAKE=" + MAKE + ", SERIES=" + SERIES_FUEL_NM + ", SEGMENT=" + SEGMENT + ", BUSINESS_MONTH="
				+ BUSINESS_MONTH + ", DEALER_STOCK=" + DEALER_STOCK + ", SALES_TARGETS=" + SALES_TARGETS
				+ ", SEASONAL_FACTOR=" + SEASONAL_FACTOR + ", FLEET_SALES=" + FLEET_SALES + ", NONFLEET_SALES="
				+ NONFLEET_SALES + ", MAX_DEALER_STOCK=" + MAX_DEALER_STOCK + ", CFTP=" + CFTP + ", HISTORICAL_SALES="
				+ HISTORICAL_SALES + ", INCENTIVES=" + INCENTIVES + ", ELASTICITY=" + ELASTICITY
				+ ", SEGMENT_CATEGORY_NM=" + SEGMENT_CATEGORY_NM + "]";
	}
	
	
}