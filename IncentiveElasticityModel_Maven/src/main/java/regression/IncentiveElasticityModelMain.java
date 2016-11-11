package regression;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.log4j.Logger;

import exceptions.MRMBaseException;
import exceptions.MRMDbPropertiesNotFoundException;
import exceptions.MRMHiveContextNotFoundException;
import exceptions.MRMInputTableDataNotFoundException;
import exceptions.MRMNoFrameDataFoundException;
import exceptions.MRMNoQueryParametersFoundException;
import exceptions.MRMNoSeriesRegionFoundException;

public class IncentiveElasticityModelMain {

	static final Logger mrmDevLogger = Logger
			.getLogger(IncentiveElasticityModelMain.class);

	public static void main(String[] args) throws IOException {
		java.util.Date date = new java.util.Date();
		Timestamp start_time = new Timestamp(date.getTime());
		IncentiveElasticityModelDTO incentiveElasticityModelDTO = new IncentiveElasticityModelDTO();
		incentiveElasticityModelDTO.setStart_time(start_time);
		
		// Calling the elasticity model.
		IncentiveElasticityModel elasticityModel = new IncentiveElasticityModel();
		try {
			elasticityModel.getIncentiveElasticityModel();
		} catch (MRMNoFrameDataFoundException e) {
			mrmDevLogger.warn(e.getMessage());
		} catch (MRMDbPropertiesNotFoundException e) {
			mrmDevLogger.warn(e.getMessage());
		} catch (MRMHiveContextNotFoundException e) {
			mrmDevLogger.warn(e.getMessage());
		} catch (MRMNoQueryParametersFoundException e) {
			mrmDevLogger.warn(e.getMessage());
		} catch (MRMInputTableDataNotFoundException e) {
			mrmDevLogger.warn(e.getMessage());
		} catch (MRMNoSeriesRegionFoundException e) {
			mrmDevLogger.warn(e.getMessage());
		} catch (MRMBaseException e) {
			mrmDevLogger.warn(e.getMessage());
		}
	}

}
