package com.toyota.analytix.mrm.msrp.dataPrep;

public class MSRPInputEntry {
	String MAKE_NM;
	String SERIES_FUEL_NM;
	String CAR_TRUCK;
	String MODEL_YEAR;
	String MONTH;
	String BUSINESS_MONTH;
	String IN_MARKET_STAGE;
	Double MONTHS_IN_MARKET;
	Double DAILY_SALES;
	Double VDW_SALES;
	Double PIN_SALES;
	Double BASE_MSRP;
	Double COMP_MSRP;
	Double YEARS_SINCE_MAJOR_CHANGE;
	Double COMP_YEARS_SINCE_MAJOR_CHANGE;
	Double INCENTIVE;
	Double SALES_SHARE;
	Double INCENTIVE_COMP_RATIO;
	Double MSRP_COMP_RATIO;
	Double COMP_INCENTIVE;
	Double CFTP;
	String LIFE_CYCLE_POSITION;
	Double MAJOR_CHANGE;
	Double MINOR_CHANGE;
	String COMP_LIFE_CYCLE_POSITION;
	Double COMP_MAJOR_CHANGE;
	Double COMP_MAJOR_CHANGE_PLUS_1;
	Double JAPAN_TSUNAMI;
	Double GAS_PRICE;
	Double GAS_PRICE_CHG_PCT;
	Double MSRP_SALES_PROD;
	Double COMP_MSRP_SALES_PROD;
	Double INCENTIVE_SALES_PROD;
	Double COMP_INCENTIVE_SALES_PROD;
	Double COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD;
	Double CFTP_SALES_PROD;
	String CREATE_TS;
	
	public MSRPInputEntry(String[] entry) {
		this.MAKE_NM = stringNotWanted(entry, MSRPInputElement.MAKE_NM);
		this.SERIES_FUEL_NM = stringNotWanted(entry, MSRPInputElement.SERIES_FUEL_NM);
		this.CAR_TRUCK = stringNotWanted(entry, MSRPInputElement.CAR_TRUCK);
		this.MODEL_YEAR = stringNotWanted(entry, MSRPInputElement.MODEL_YEAR);
		this.MONTH = stringNotWanted(entry, MSRPInputElement.MONTH);
		this.BUSINESS_MONTH = stringNotWanted(entry, MSRPInputElement.BUSINESS_MONTH);
		this.IN_MARKET_STAGE = stringNotWanted(entry, MSRPInputElement.IN_MARKET_STAGE);
		this.MONTHS_IN_MARKET = doubleNotWanted(entry, MSRPInputElement.MONTHS_IN_MARKET);
		this.DAILY_SALES = doubleNotWanted(entry, MSRPInputElement.DAILY_SALES);
		this.VDW_SALES = doubleNotWanted(entry, MSRPInputElement.VDW_SALES);
		this.BASE_MSRP = doubleNotWanted(entry, MSRPInputElement.BASE_MSRP);
		this.COMP_MSRP = doubleNotWanted(entry, MSRPInputElement.COMP_MSRP);
		this.YEARS_SINCE_MAJOR_CHANGE = doubleNotWanted(entry, MSRPInputElement.YEARS_SINCE_MAJOR_CHANGE);
		this.COMP_YEARS_SINCE_MAJOR_CHANGE = doubleNotWanted(entry, MSRPInputElement.COMP_YEARS_SINCE_MAJOR_CHANGE);
		this.INCENTIVE = doubleNotWanted(entry, MSRPInputElement.INCENTIVE);
		this.SALES_SHARE = doubleNotWanted(entry, MSRPInputElement.SALES_SHARE);
		this.INCENTIVE_COMP_RATIO = doubleNotWanted(entry, MSRPInputElement.INCENTIVE_COMP_RATIO);
		this.MSRP_COMP_RATIO = doubleNotWanted(entry, MSRPInputElement.MSRP_COMP_RATIO);
		this.COMP_INCENTIVE = doubleNotWanted(entry, MSRPInputElement.COMP_INCENTIVE);
		this.CFTP = doubleNotWanted(entry, MSRPInputElement.CFTP);
		this.LIFE_CYCLE_POSITION = stringNotWanted(entry, MSRPInputElement.LIFE_CYCLE_POSITION);
		this.MAJOR_CHANGE = doubleNotWanted(entry, MSRPInputElement.MAJOR_CHANGE);
		this.MINOR_CHANGE = doubleNotWanted(entry, MSRPInputElement.MINOR_CHANGE);
		this.COMP_LIFE_CYCLE_POSITION = stringNotWanted(entry, MSRPInputElement.COMP_LIFE_CYCLE_POSITION);
		this.COMP_MAJOR_CHANGE = doubleNotWanted(entry, MSRPInputElement.COMP_MAJOR_CHANGE);
		this.COMP_MAJOR_CHANGE_PLUS_1 = doubleNotWanted(entry, MSRPInputElement.COMP_MAJOR_CHANGE_PLUS_1);
		this.JAPAN_TSUNAMI = doubleNotWanted(entry, MSRPInputElement.JAPAN_TSUNAMI);
		this.GAS_PRICE = doubleNotWanted(entry, MSRPInputElement.GAS_PRICE);
		this.GAS_PRICE_CHG_PCT = doubleNotWanted(entry, MSRPInputElement.GAS_PRICE_CHG_PCT);
		
		this.PIN_SALES = doubleNotWanted(entry, MSRPInputElement.PIN_SALES);
		this.MSRP_SALES_PROD = doubleNotWanted(entry, MSRPInputElement.MSRP_SALES_PROD);
		this.COMP_INCENTIVE_SALES_PROD = doubleNotWanted(entry, MSRPInputElement.COMP_INCENTIVE_SALES_PROD);
		this.INCENTIVE_SALES_PROD = doubleNotWanted(entry, MSRPInputElement.INCENTIVE_SALES_PROD);
		this.COMP_INCENTIVE_SALES_PROD = doubleNotWanted(entry, MSRPInputElement.COMP_INCENTIVE_SALES_PROD);
		this.COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD = doubleNotWanted(entry, MSRPInputElement.COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD);
		this.CFTP_SALES_PROD = doubleNotWanted(entry, MSRPInputElement.CFTP_SALES_PROD);
		//this.CREATE_TS = stringNotWanted(entry, MSRPInputElement.CREATE_TS);
		
	}
	
	public Double doubleNotWanted(String[] entry, MSRPInputElement mmie) {
		if (entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toUpperCase().equals("NULL") || 
				entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toUpperCase().equals("")) {
			return null;
		}
		return Double.parseDouble(entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toString());
	}
	
	public Integer intNotWanted(String[] entry, MSRPInputElement mmie) {
		if (entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toUpperCase().equals("NULL") || 
				entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toUpperCase().equals("")) {
			return 0;
		}
		return (int) Double.parseDouble(entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toString());
	}
	
	public String stringNotWanted(String[] entry, MSRPInputElement mmie) {
		if (entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toUpperCase().equals("NULL")) {
			return null;
		}
		return entry[new MSRPInputElementDecoder(mmie).decodeIndex()].toString();
	}

	public String getMAKE_NM() {
		return MAKE_NM;
	}

	public void setMAKE_NM(String mAKE_NM) {
		MAKE_NM = mAKE_NM;
	}

	public String getSERIES_FUEL_NM() {
		return SERIES_FUEL_NM;
	}

	public void setSERIES_FUEL_NM(String sERIES_FUEL_NM) {
		SERIES_FUEL_NM = sERIES_FUEL_NM;
	}

	public String getCAR_TRUCK() {
		return CAR_TRUCK;
	}

	public void setCAR_TRUCK(String cAR_TRUCK) {
		CAR_TRUCK = cAR_TRUCK;
	}

	

	public Double getBASE_MSRP() {
		return BASE_MSRP;
	}

	public void setBASE_MSRP(Double bASE_MSRP) {
		BASE_MSRP = bASE_MSRP;
	}

	public Double getCOMP_MSRP() {
		return COMP_MSRP;
	}

	public void setCOMP_MSRP(Double cOMP_MSRP) {
		COMP_MSRP = cOMP_MSRP;
	}

	public Double getINCENTIVE() {
		return INCENTIVE;
	}

	public void setINCENTIVE(Double iNCENTIVE) {
		INCENTIVE = iNCENTIVE;
	}

	public Double getSALES_SHARE() {
		return SALES_SHARE;
	}

	public void setSALES_SHARE(Double sALES_SHARE) {
		SALES_SHARE = sALES_SHARE;
	}

	public Double getINCENTIVE_COMP_RATIO() {
		return INCENTIVE_COMP_RATIO;
	}

	public void setINCENTIVE_COMP_RATIO(Double iNCENTIVE_COMP_RATIO) {
		INCENTIVE_COMP_RATIO = iNCENTIVE_COMP_RATIO;
	}

	public Double getMSRP_COMP_RATIO() {
		return MSRP_COMP_RATIO;
	}

	public void setMSRP_COMP_RATIO(Double mSRP_COMP_RATIO) {
		MSRP_COMP_RATIO = mSRP_COMP_RATIO;
	}

	public Double getCOMP_INCENTIVE() {
		return COMP_INCENTIVE;
	}

	public void setCOMP_INCENTIVE(Double cOMP_INCENTIVE) {
		COMP_INCENTIVE = cOMP_INCENTIVE;
	}

	public Double getCFTP() {
		return CFTP;
	}

	public void setCFTP(Double cFTP) {
		CFTP = cFTP;
	}

	public String getLIFE_CYCLE_POSITION() {
		return LIFE_CYCLE_POSITION;
	}

	public void setLIFE_CYCLE_POSITION(String lIFE_CYCLE_POSITION) {
		LIFE_CYCLE_POSITION = lIFE_CYCLE_POSITION;
	}



	public String getCOMP_LIFE_CYCLE_POSITION() {
		return COMP_LIFE_CYCLE_POSITION;
	}

	public void setCOMP_LIFE_CYCLE_POSITION(String cOMP_LIFE_CYCLE_POSITION) {
		COMP_LIFE_CYCLE_POSITION = cOMP_LIFE_CYCLE_POSITION;
	}



	public Double getGAS_PRICE() {
		return GAS_PRICE;
	}

	public void setGAS_PRICE(Double gAS_PRICE) {
		GAS_PRICE = gAS_PRICE;
	}

	public Double getGAS_PRICE_CHG_PCT() {
		return GAS_PRICE_CHG_PCT;
	}

	public void setGAS_PRICE_CHG_PCT(Double gAS_PRICE_CHG_PCT) {
		GAS_PRICE_CHG_PCT = gAS_PRICE_CHG_PCT;
	}

	@Override
	public String toString() {
		return "MSRPInputEntry [MAKE_NM=" + MAKE_NM + ", SERIES_FUEL_NM=" + SERIES_FUEL_NM + ", CAR_TRUCK=" + CAR_TRUCK
				+ ", MODEL_YEAR=" + MODEL_YEAR + ", MONTH=" + MONTH + ", BUSINESS_MONTH=" + BUSINESS_MONTH
				+ ", IN_MARKET_STAGE=" + IN_MARKET_STAGE + ", MONTHS_IN_MARKET=" + MONTHS_IN_MARKET + ", DAILY_SALES="
				+ DAILY_SALES + ", VDW_SALES=" + VDW_SALES + ", BASE_MSRP=" + BASE_MSRP + ", COMP_MSRP=" + COMP_MSRP
				+ ", YEARS_SINCE_MAJOR_CHANGE=" + YEARS_SINCE_MAJOR_CHANGE + ", COMP_YEARS_SINCE_MAJOR_CHANGE="
				+ COMP_YEARS_SINCE_MAJOR_CHANGE + ", INCENTIVE=" + INCENTIVE + ", SALES_SHARE=" + SALES_SHARE
				+ ", INCENTIVE_COMP_RATIO=" + INCENTIVE_COMP_RATIO + ", MSRP_COMP_RATIO=" + MSRP_COMP_RATIO
				+ ", COMP_INCENTIVE=" + COMP_INCENTIVE + ", CFTP=" + CFTP + ", LIFE_CYCLE_POSITION="
				+ LIFE_CYCLE_POSITION + ", MAJOR_CHANGE=" + MAJOR_CHANGE + ", MINOR_CHANGE=" + MINOR_CHANGE
				+ ", COMP_LIFE_CYCLE_POSITION=" + COMP_LIFE_CYCLE_POSITION + ", COMP_MAJOR_CHANGE=" + COMP_MAJOR_CHANGE
				+ ", COMP_MAJOR_CHANGE_PLUS_1=" + COMP_MAJOR_CHANGE_PLUS_1 + ", JAPAN_TSUNAMI=" + JAPAN_TSUNAMI
				+ ", GAS_PRICE=" + GAS_PRICE + ", GAS_PRICE_CHG_PCT=" + GAS_PRICE_CHG_PCT + "]";
	}
	
	public String toCSVString() {
		return this.MAKE_NM + "|" + this.SERIES_FUEL_NM + "|" + this.CAR_TRUCK + "|" + this.MODEL_YEAR + "|" + this.MONTH + "|" + this.BUSINESS_MONTH + "|" + 
				this.IN_MARKET_STAGE + "|" + this.MONTHS_IN_MARKET + "|" + this.DAILY_SALES + "|" + this.VDW_SALES + "|" + this.BASE_MSRP + "|" + this.COMP_MSRP + "|" + 
				this.YEARS_SINCE_MAJOR_CHANGE + "|" + this.COMP_YEARS_SINCE_MAJOR_CHANGE + "|" + this.INCENTIVE + "|" + this.SALES_SHARE + "|" + this.INCENTIVE_COMP_RATIO + "|" + 
				this.MSRP_COMP_RATIO + "|" + this.COMP_INCENTIVE + "|" + this.CFTP + "|" + this.LIFE_CYCLE_POSITION + "|" + this.MAJOR_CHANGE + "|" + this.MINOR_CHANGE + "|" + 
				this.COMP_LIFE_CYCLE_POSITION + "|" + this.COMP_MAJOR_CHANGE + "|" + this.COMP_MAJOR_CHANGE_PLUS_1 + "|" + this.JAPAN_TSUNAMI + "|" + this.GAS_PRICE + "|" + this.GAS_PRICE_CHG_PCT;
	}
	
	public Object[] toObjectArray() {
		Object[] obj = { this.MAKE_NM, this.SERIES_FUEL_NM, this.CAR_TRUCK, this.MODEL_YEAR, this.MONTH,
				this.BUSINESS_MONTH, this.IN_MARKET_STAGE, this.MONTHS_IN_MARKET, this.DAILY_SALES, this.VDW_SALES,
				this.PIN_SALES, this.BASE_MSRP, this.COMP_MSRP, this.YEARS_SINCE_MAJOR_CHANGE,
				this.COMP_YEARS_SINCE_MAJOR_CHANGE, this.INCENTIVE, this.SALES_SHARE, this.INCENTIVE_COMP_RATIO,
				this.MSRP_COMP_RATIO, this.COMP_INCENTIVE, this.CFTP, this.LIFE_CYCLE_POSITION, this.MAJOR_CHANGE,
				this.MINOR_CHANGE, this.COMP_LIFE_CYCLE_POSITION, this.COMP_MAJOR_CHANGE, this.COMP_MAJOR_CHANGE_PLUS_1,
				this.JAPAN_TSUNAMI, this.GAS_PRICE, this.GAS_PRICE_CHG_PCT, this.MSRP_SALES_PROD,
				this.COMP_MSRP_SALES_PROD, this.INCENTIVE_SALES_PROD, this.COMP_INCENTIVE_SALES_PROD,
				this.COMPETITOR_YRS_SINCE_MAJOR_CHANGE_SALES_PROD, this.CFTP_SALES_PROD };
		return obj;
	}
}
