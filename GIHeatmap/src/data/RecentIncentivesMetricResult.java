package data;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;

public class RecentIncentivesMetricResult implements Serializable {
	String make;
	String series;
	String segment;
	int business_month; 
	double recentIncentivesMetric;
	double cftp; 
	double incentives;
	double series_monthly_inctv_pct, segment_monthly_inctv_pct, series_average_inctv_pct,segment_avg_monthly_inctv_pct;
	static int rollingMonthCount = 6;
	private static final Logger logger = Logger.getLogger(RecentIncentivesMetricResult.class);
	
	public RecentIncentivesMetricResult(String make, String series, String segment, int business_month, double cftp,
			double incentives) {
		super();
		this.make = make;
		this.series = series;
		this.business_month = business_month;
		this.cftp = cftp;
		this.incentives = incentives;
		this.segment = segment;
	}
	
	public void calculateRecentIncentivesMetric() {
		logger.debug("calculateRecentIncentivesMetric");
		
		this.series_monthly_inctv_pct = this.incentives / this.cftp;
		
		this.recentIncentivesMetric = (series_monthly_inctv_pct / segment_monthly_inctv_pct) / (series_average_inctv_pct / segment_avg_monthly_inctv_pct);
	}

	public double calculateSeriesAvgInctvPct(ArrayList<HeatMapDataEntry> hmdes) {
		double averageInctvPct = 0.0;
		int totalHistoricalSales = 0;
		
		int minBusinessMonth = Integer.parseInt(new SimpleDateFormat("yyyyMM").format(DateUtils.addMonths(new Date(this.business_month),  rollingMonthCount)));
		
		for (HeatMapDataEntry hmde : hmdes) {
			if (hmde.SERIES_FUEL_NM.equals(this.series)) {
				if (hmde.BUSINESS_MONTH > minBusinessMonth && hmde.BUSINESS_MONTH <= this.business_month) {
					if (hmde.CFTP > 0 && hmde.INCENTIVES >= 0 && hmde.HISTORICAL_SALES > 0) {

						averageInctvPct += (hmde.INCENTIVES / hmde.CFTP) * hmde.HISTORICAL_SALES;
						totalHistoricalSales += hmde.HISTORICAL_SALES;	
					}
				}
			}
		}
		
		this.series_average_inctv_pct = averageInctvPct / totalHistoricalSales;
		
		System.out.println("Calculated SeriesAvgInctvPct: " + this.series_average_inctv_pct);
		return this.series_average_inctv_pct;
	}
	
	public double calculateSegmentMonthlyInctvPct(ArrayList<HeatMapDataEntry> hmdes) {
		double monthlyInctvPct = 0.0;
		int totalHistoricalSales = 0;
		
		for (HeatMapDataEntry hmde : hmdes) {
			if (!hmde.SERIES_FUEL_NM.equals(this.series)) { // if not 'our' series_fuel_nm
				
				if (hmde.SEGMENT.equals(this.segment) && hmde.BUSINESS_MONTH == this.business_month) {
					if (hmde.CFTP > 0 && hmde.INCENTIVES >= 0 && hmde.HISTORICAL_SALES > 0) {

						monthlyInctvPct += (hmde.INCENTIVES / hmde.CFTP) * hmde.HISTORICAL_SALES;
						totalHistoricalSales += hmde.HISTORICAL_SALES;	
					}
				}
			}
		}
		
		this.segment_monthly_inctv_pct = monthlyInctvPct / totalHistoricalSales;
		
		return this.segment_monthly_inctv_pct;
	}
	
	public double calculateSegmentAvgCFTPRatio(ArrayList<HeatMapDataEntry> hmdes) {
		double avgMonthlyInctvPct = 0.0;
		int totalHistoricalSales = 0;
		
		int minBusinessMonth = Integer.parseInt(new SimpleDateFormat("yyyyMM").format(DateUtils.addMonths(new Date(this.business_month),  rollingMonthCount)));
		
		for (HeatMapDataEntry hmde : hmdes) {
			if (!hmde.SERIES_FUEL_NM.equals(this.series)) {
				if (hmde.SEGMENT.equals(this.segment)) {
					if (hmde.BUSINESS_MONTH > minBusinessMonth && hmde.BUSINESS_MONTH <= this.business_month) {
						if (hmde.CFTP > 0 && hmde.INCENTIVES >= 0 && hmde.HISTORICAL_SALES > 0) {
							
							avgMonthlyInctvPct += (hmde.INCENTIVES / hmde.CFTP) * hmde.HISTORICAL_SALES;
							totalHistoricalSales += hmde.HISTORICAL_SALES;	
							
						}
					}
				}
			}
		}
		
		this.segment_avg_monthly_inctv_pct =  avgMonthlyInctvPct / totalHistoricalSales;
		
		return this.segment_avg_monthly_inctv_pct;
	}
	
	public String getSeries() {
		return series;
	}

	public void setSeries(String series) {
		this.series = series;
	}

	public int getBusiness_month() {
		return business_month;
	}

	public void setBusiness_month(int business_month) {
		this.business_month = business_month;
	}

	public double getRecentIncentivesMetric() {
		return recentIncentivesMetric;
	}

	public void setRecentIncentivesMetric(double recentIncentivesMetric) {
		this.recentIncentivesMetric = recentIncentivesMetric;
	}

	public double getCftp() {
		return cftp;
	}

	public void setCftp(double cftp) {
		this.cftp = cftp;
	}

	public double getIncentives() {
		return incentives;
	}

	public void setIncentives(double incentives) {
		this.incentives = incentives;
	}

	public double getSeries_monthly_cftp_ratio() {
		return series_monthly_inctv_pct;
	}

	public void setSeries_monthly_cftp_ratio(double series_monthly_cftp_ratio) {
		this.series_monthly_inctv_pct = series_monthly_cftp_ratio;
	}

	public double getSegment_monthly_cftp_ratio() {
		return segment_monthly_inctv_pct;
	}

	public void setSegment_monthly_cftp_ratio(double segment_monthly_cftp_ratio) {
		this.segment_monthly_inctv_pct = segment_monthly_cftp_ratio;
	}

	public double getSeries_average_cftp_ratio() {
		return series_average_inctv_pct;
	}

	public void setSeries_average_cftp_ratio(double series_average_cftp_ratio) {
		this.series_average_inctv_pct = series_average_cftp_ratio;
	}

	public double getSegment_average_cftp_ratio() {
		return segment_avg_monthly_inctv_pct;
	}

	public void setSegment_average_cftp_ratio(double segment_average_cftp_ratio) {
		this.segment_avg_monthly_inctv_pct = segment_average_cftp_ratio;
	}

	@Override
	public String toString() {
		return "RecentIncentivesMetricResult [series=" + series + ", segment=" + segment + ", business_month="
				+ business_month + ", recentIncentivesMetric=" + recentIncentivesMetric + ", cftp=" + cftp
				+ ", incentives=" + incentives + ", series_monthly_cftp_ratio=" + series_monthly_inctv_pct
				+ ", segment_monthly_cftp_ratio=" + segment_monthly_inctv_pct + ", series_average_cftp_ratio="
				+ series_average_inctv_pct + ", segment_average_cftp_ratio=" + segment_avg_monthly_inctv_pct + "]";
	}
}
