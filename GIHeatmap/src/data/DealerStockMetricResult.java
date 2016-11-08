package data;

import java.io.Serializable;

public class DealerStockMetricResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4412114561939893974L;
	String series;
	int business_month;
	double dealer_stock_metric;
	
	public DealerStockMetricResult(String series, int business_month, double dealer_stock_metric) {
		super();
		this.series = series;
		this.business_month = business_month;
		this.dealer_stock_metric = dealer_stock_metric;
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
	public double getDealerStockMetric() {
		return dealer_stock_metric;
	}
	public void setDealer_stock_metric(int dealer_stock_metric) {
		this.dealer_stock_metric = dealer_stock_metric;
	}


	@Override
	public String toString() {
		return "DealerStockMetricResult [series=" + series + ", business_month=" + business_month
				+ ", dealer_stock_metric=" + dealer_stock_metric + "]";
	}
	
	
}
