package data;

import java.io.Serializable;

import org.apache.log4j.Logger;
/*
 * HeatMapElementDecoder is a class designed to help ease the change of column positions in data for each element that is needed for the GI model.
 * 
 * i.e. the returned value of 'decodeIndex()' is the current index in which that element can be found in the return data
 */
public class HeatMapElementDecoder implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1494723348773654485L;
	private static final Logger logger = Logger.getLogger(HeatMapElementDecoder.class);
	HeatMapElement element;
	
	public HeatMapElementDecoder(HeatMapElement element) {
		this.element = element;
	}
	
	public int decodeIndex() {
		switch (element) {
		case MAKE:
			return 0;
		case SERIES_FUEL_NM:
			return 1;
		case SEGMENT:
			return 2;
		case SEGMENT_CATEGORY_NM:
			return 3;
		case BUSINESS_MONTH:
			return 4;
		case DEALER_STOCK:
			return 5;
		case SALES_TARGETS:
			return 8;
		case SEASONAL_FACTOR:
			return 9;
		case FLEET_SALES:
			return 6;
		case NONFLEET_SALES:
			return 7;
		case MAX_DEALER_STOCK:
			return 10;
		case CFTP:
			return 11;
		case HISTORICAL_SALES:
			return 12;
		case INCENTIVES:
			return 13;
		case ELASTICITY:
			return 14;
		}

		System.out.println("Index of GI Input element is incorrect");
		return -1;
	}
}
