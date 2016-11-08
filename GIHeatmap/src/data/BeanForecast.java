package data;

import java.io.Serializable;

public class BeanForecast implements Serializable {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8390399411232723990L;
	private String RAW_SERIES_NM;
	private String SERIES_NM;
	private String SEGMENT_NM;
	private String SEGMENT_CATEGORY_NM;
	private String RAW_REGION_CD;
	private String REGION_AREA_NM;
	private String REGION_AREA_CD;
	private String REGION_LONG_NM;
	private String SALES_GEOG_TYPE_CD;
	private String REGION_CHAR_CD;
	private String BUSINESS_MONTH;
	private String BUSINESS_METRIC;
	private String SCORE;
	
	
	public BeanForecast(String RAW_SERIES_NM, String sERIES_NM, String sEGMENT_NM, String sEGMENT_CATEGORY_NM,
			String rAW_REGION_CD, String rEGION_AREA_NM, String rEGION_AREA_CD, String rEGION_LONG_NM,
			String sALES_GEOG_TYPE_CD, String rEGION_CHAR_CD, String bUSINESS_MONTH, String bUSINESS_METRIC,
			String sCORE) {
		
		super();
		this.RAW_SERIES_NM = RAW_SERIES_NM;
		this.SERIES_NM = sERIES_NM;
		this.SEGMENT_NM = sEGMENT_NM;
		this.SEGMENT_CATEGORY_NM = sEGMENT_CATEGORY_NM;
		this.RAW_REGION_CD = rAW_REGION_CD;
		this.REGION_AREA_NM = rEGION_AREA_NM;
		this.REGION_AREA_CD = rEGION_AREA_CD;
		this.REGION_LONG_NM = rEGION_LONG_NM;
		this.SALES_GEOG_TYPE_CD = sALES_GEOG_TYPE_CD;
		this.REGION_CHAR_CD = rEGION_CHAR_CD;
		this.BUSINESS_MONTH = bUSINESS_MONTH;
		this.BUSINESS_METRIC = bUSINESS_METRIC;
		this.SCORE = sCORE;
	}


	public String getRAW_SERIES_NM() {
		return RAW_SERIES_NM;
	}


	public void setRAW_SERIES_NM(String rAW_SERIES_NM) {
		RAW_SERIES_NM = rAW_SERIES_NM;
	}


	public String getSERIES_NM() {
		return SERIES_NM;
	}


	public void setSERIES_NM(String sERIES_NM) {
		SERIES_NM = sERIES_NM;
	}


	public String getSEGMENT_NM() {
		return SEGMENT_NM;
	}


	public void setSEGMENT_NM(String sEGMENT_NM) {
		SEGMENT_NM = sEGMENT_NM;
	}


	public String getSEGMENT_CATEGORY_NM() {
		return SEGMENT_CATEGORY_NM;
	}


	public void setSEGMENT_CATEGORY_NM(String sEGMENT_CATEGORY_NM) {
		SEGMENT_CATEGORY_NM = sEGMENT_CATEGORY_NM;
	}


	public String getRAW_REGION_CD() {
		return RAW_REGION_CD;
	}


	public void setRAW_REGION_CD(String rAW_REGION_CD) {
		RAW_REGION_CD = rAW_REGION_CD;
	}


	public String getREGION_AREA_NM() {
		return REGION_AREA_NM;
	}


	public void setREGION_AREA_NM(String rEGION_AREA_NM) {
		REGION_AREA_NM = rEGION_AREA_NM;
	}


	public String getREGION_AREA_CD() {
		return REGION_AREA_CD;
	}


	public void setREGION_AREA_CD(String rEGION_AREA_CD) {
		REGION_AREA_CD = rEGION_AREA_CD;
	}


	public String getREGION_LONG_NM() {
		return REGION_LONG_NM;
	}


	public void setREGION_LONG_NM(String rEGION_LONG_NM) {
		REGION_LONG_NM = rEGION_LONG_NM;
	}


	public String getSALES_GEOG_TYPE_CD() {
		return SALES_GEOG_TYPE_CD;
	}


	public void setSALES_GEOG_TYPE_CD(String sALES_GEOG_TYPE_CD) {
		SALES_GEOG_TYPE_CD = sALES_GEOG_TYPE_CD;
	}


	public String getREGION_CHAR_CD() {
		return REGION_CHAR_CD;
	}


	public void setREGION_CHAR_CD(String rEGION_CHAR_CD) {
		REGION_CHAR_CD = rEGION_CHAR_CD;
	}


	public String getBUSINESS_MONTH() {
		return BUSINESS_MONTH;
	}


	public void setBUSINESS_MONTH(String bUSINESS_MONTH) {
		BUSINESS_MONTH = bUSINESS_MONTH;
	}


	public String getBUSINESS_METRIC() {
		return BUSINESS_METRIC;
	}


	public void setBUSINESS_METRIC(String bUSINESS_METRIC) {
		BUSINESS_METRIC = bUSINESS_METRIC;
	}


	public String getSCORE() {
		return SCORE;
	}


	public void setSCORE(String sCORE) {
		SCORE = sCORE;
	}
	
}
