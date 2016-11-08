package data;

import java.io.Serializable;

public class TargetVsPaceResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 850109862643936339L;
	String series;
	int business_month;
	double tvp;
	
	public TargetVsPaceResult(String series, int business_month, double tvp) {
		super();
		this.series = series;
		this.business_month = business_month;
		this.tvp = tvp;
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
	public double getTvp() {
		return tvp;
	}
	public void setTvp(double tvp) {
		this.tvp = tvp;
	}
	@Override
	public String toString() {
		return "TargetVsPaceResult [series=" + series + ", business_month=" + business_month + ", tvp=" + tvp + "]";
	}
	
	
}
