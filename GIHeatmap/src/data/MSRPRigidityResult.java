package data;

import java.io.Serializable;

public class MSRPRigidityResult implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -8447106564398590218L;
	String series;
	int business_month;
	double msrpr;
	
	public MSRPRigidityResult(String series, int business_month, double msrpr) {
		super();
		this.series = series;
		this.business_month = business_month;
		this.msrpr = msrpr;
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
	public double getRigidity() {
		return msrpr;
	}
	public void setRigidity(int msrpr) {
		this.msrpr = msrpr;
	}

	@Override
	public String toString() {
		return "MSRPRigidityResult [series=" + series + ", business_month=" + business_month + ", msrpr=" + msrpr + "]";
	}
	
	
}
