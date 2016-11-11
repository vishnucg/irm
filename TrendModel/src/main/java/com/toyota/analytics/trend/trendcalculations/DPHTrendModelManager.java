/**
 * 
 */
package com.toyota.analytics.trend.trendcalculations;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;

import com.toyota.analytics.common.exceptions.AnalyticsRuntimeException;

/**
 * This class contains the methods which will do DPH trending calculations. We
 * will calculate the DPH Value based on the previous row of each series. For
 * each series, we will take last DPH Value from history frame and will copy
 * over to future frame.
 * 
 * @author Naresh
 *
 */
public class DPHTrendModelManager implements Serializable {

	private static final long serialVersionUID = 3674086249543581364L;

	// initialize the logger.
	private static final Logger logger = Logger
			.getLogger(DPHTrendModelManager.class);

	/**
	 * This method will do DPH trending calculations. We will calculate the DPH
	 * Value based on the previous row of each series. For each series, we will
	 * take last DPH Value from history frame and will copy over to future
	 * frame.
	 * 
	 * @param trendInputFrame
	 * @param previousDphValue
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	public List<Double> calculateDPHTrend(DataFrame trendInputFrame,
			double previousDphValue) throws IOException,
			AnalyticsRuntimeException {
		List<Double> forecastValues;

		// Calculate predicted PDH Values.
		forecastValues = predictDPHValues(trendInputFrame, previousDphValue);
		return forecastValues;
	}

	/**
	 * This method adds the previous DPH value as the current DPH value.
	 * 
	 * @param trendInput
	 * @param previousDphValue
	 * @return
	 * @throws IOException
	 * @throws AnalyticsRuntimeException
	 */
	private List<Double> predictDPHValues(DataFrame trendInput,
			double previousDphValue) throws IOException,
			AnalyticsRuntimeException {

		List<Double> forecastDphValuesList = new ArrayList<>();
		for (int i = 0; i < trendInput.count(); i++) {
			forecastDphValuesList.add((double) Math
					.round(previousDphValue / 50) * 50);
		}
		logger.info("forecastDphValues: " + forecastDphValuesList);
		return forecastDphValuesList;
	}
}
