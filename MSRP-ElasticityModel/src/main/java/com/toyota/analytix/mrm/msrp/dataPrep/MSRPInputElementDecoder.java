package com.toyota.analytix.mrm.msrp.dataPrep;

public class MSRPInputElementDecoder {
	MSRPInputElement element;
	
	public MSRPInputElementDecoder(MSRPInputElement element) {
		super();
		this.element = element;
	}
	
	public int decodeIndex() {
		switch (this.element) {
			case MAKE_NM:
				return 0;
			case SERIES_FUEL_NM:
				return 1;
			case CAR_TRUCK:
				return 2;
			case MODEL_YEAR:
				return 3;
			case MONTH:
				return 4;
			case BUSINESS_MONTH:
				return 5;
			case IN_MARKET_STAGE:
				return 6;
			case MONTHS_IN_MARKET:
				return 7;
			case DAILY_SALES:
				return 8;
			case VDW_SALES:
				return 9;
			case PIN_SALES:
				return 9;
			case BASE_MSRP:
				return 11;
			case COMP_MSRP:
				return 12;
			case YEARS_SINCE_MAJOR_CHANGE:
				return 13;
			case COMP_YEARS_SINCE_MAJOR_CHANGE:
				return 14;
			case INCENTIVE:
				return 15;
			case SALES_SHARE:
				return 16;
			case INCENTIVE_COMP_RATIO:
				return 17;
			case MSRP_COMP_RATIO:
				return 18;
			case COMP_INCENTIVE:
				return 19;
			case CFTP:
				return 20;
			case LIFE_CYCLE_POSITION:
				return 21;
			case MAJOR_CHANGE:
				return 22;
			case MINOR_CHANGE:
				return 23;
			case COMP_LIFE_CYCLE_POSITION:
				return 24;
			case COMP_MAJOR_CHANGE:
				return 25;
			case COMP_MAJOR_CHANGE_PLUS_1:
				return 26;
			case JAPAN_TSUNAMI:
				return 27;
			case GAS_PRICE:
				return 28;
			case GAS_PRICE_CHG_PCT:
				return 29;
			case MSRP_SALES_PROD:
				return 30;
			case COMP_MSRP_SALES_PROD:
				return 31;
			case INCENTIVE_SALES_PROD:
				return 32;
			case COMP_INCENTIVE_SALES_PROD:
				return 33;
			case COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD:
				return 34;
			case CFTP_SALES_PROD:
				return 35;
		default:
			break;
		}
		
		System.out.println("Index of MSRP MRM Input element is incorrect");
		return -1;
	}
}
