/**
 * 
 */
package variableSelection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import regression.LinearModel;
import util.MRMIndexes;
import exceptions.MRMBaseException;
import exceptions.MRMCoefficientsNotFoundException;
import exceptions.MRMColumnNotFoundException;
import exceptions.MRMHeaderNotFoundException;
import exceptions.MRMLinearModelNotFoundException;
import exceptions.MRMNoDataFoundException;
import exceptions.MRMNoIndependentVariablesFoundException;
import exceptions.MRMPValuesNotFoundException;
import exceptions.MRMPropertiesNotFoundException;
import exceptions.MRMVariablesNotFoundException;
import exceptions.MRMVifNotFoundException;

/**
 * This class contains the methods which will give the variables back based on
 * the variable selection module. This takes the required properties. As per
 * this variable selection module, this will check if there any errors in the
 * data set as per the error checking step. If we found any errors with
 * particular columns those will be removed in this error check. Once passed the
 * error check stage then we got the data set and will take VIF Values of
 * particular data set. If VIF Value of Inclusive variable is greater than
 * threshold value, next take the VIF values of all variables and exclude the
 * variable which has high value. We will take maximum value and will exclude
 * that variable from the list. If VIF Value is less than threshold value, then
 * will take the all variables. Once we got the variables , will remove
 * unreasonable coefficients variables on the VIF Output list. Once we got the
 * list , then finally will take the variables which are having the p-value is
 * less than threshold value. Finally this will return the list of variables,
 * which are passed all these steps.
 * 
 * @author Naresh
 *
 */
public class VariableSelection implements Serializable {

	private static final long serialVersionUID = 8450540479985678384L;
	// Initializing the logger.
	static final Logger mrmDevLogger = Logger
			.getLogger(VariableSelection.class);

	/**
	 * This method takes the required properties. As per this variable selection
	 * module, this will check if there any errors in the data set as per the
	 * error checking step. If we found any errors with particular columns those
	 * will be removed in this error check. Once passed the error check stage
	 * then we got the data set and will take VIF Values of particular data set.
	 * If VIF Value of Inclusive variable is greater than threshold value, next
	 * take the VIF values of all variables and exclude the variable which has
	 * high value. We will take maximum value and will exclude that variable
	 * from the list. If VIF Value is less than threshold value, then will take
	 * the all variables. Once we got the variables , will remove unreasonable
	 * coefficients variables on the VIF Output list. Once we got the list ,
	 * then finally will take the variables which are having the p-value is less
	 * than threshold value. Finally this will return the list of variables,
	 * which are passed all these steps.
	 * 
	 * @param dataRDD
	 * @param propertiesValues
	 * @param sparkContext
	 * @param cutOff
	 * @return vifValues
	 * @throws MRMBaseException
	 */
	public Map<String, Double> getVariables(JavaRDD<String> dataRDD,
			Map<String, String> propertiesValues,
			JavaSparkContext sparkContext, int cutOff) throws MRMBaseException {
		Map<String, Double> vifValues = null;
		VariableSelectionPropertiesDTO variableSelectionDTO;
		mrmDevLogger.warn("Variable selection started at " + new Date());
		mrmDevLogger.warn(propertiesValues);

		// Checks input data is available or not.
		if (null == dataRDD || dataRDD.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			// Taken the rows count from the input file.
			int noOfLines = (int) (dataRDD.count());

			// Read the input properties.
			variableSelectionDTO = readProperties(propertiesValues);

			// Collect the header information.
			Map<String, Integer> headersInformation = MRMIndexes
					.getHeadersInformation(dataRDD);
			variableSelectionDTO.setHeadersInformation(headersInformation);
			variableSelectionDTO.setNoOfLines(noOfLines);

			// Header check.
			if (null == headersInformation || headersInformation.isEmpty()) {
				mrmDevLogger.error(MRMIndexes.getValue("COLUMN_HEADER_WARN"));
				throw new MRMHeaderNotFoundException(
						MRMIndexes.getValue("COLUMN_HEADER_WARN"));
			} else {
				// Remove columns with not enough unique values and Error -
				// singularity matrix - check
				List<String> validDataList = checkUniqueValues(dataRDD,
						variableSelectionDTO,
						variableSelectionDTO.getHeadersInformation());
				variableSelectionDTO.setErrorFreeList(validDataList);
				mrmDevLogger.warn("Valid data list: " + validDataList);

				// independent variables including the inclusive variable >
				// number of rows
				if (noOfLines < variableSelectionDTO.getIndependentColumnList()
						.size() + 1) {
					mrmDevLogger.warn(MRMIndexes.getValue("LESS_RECORDS"));

				} else {
					// Calculating the VIF Values.
					variableSelectionDTO = calculateVifValues(
							variableSelectionDTO, dataRDD, cutOff);
					mrmDevLogger.warn("VIF Values list: "
							+ variableSelectionDTO.getVifValues());

					// Remove unreasonable coefficients list.
					variableSelectionDTO = removeUnresonableCoefList(variableSelectionDTO);
					mrmDevLogger.warn("After removeUnresonableCoefList: "
							+ variableSelectionDTO.getVifValues());

					// Remove insignificant variables list.
					variableSelectionDTO = removeInsignificantVars(variableSelectionDTO);
					mrmDevLogger.warn("After removeInsignificantVars: "
							+ variableSelectionDTO.getVifValues());

					// Returns the final variables list.
					vifValues = variableSelectionDTO.getVifValues();
					mrmDevLogger.warn("Final variables list: "
							+ variableSelectionDTO.getVifValues());
				}
			}
		}
		mrmDevLogger.warn("Variable selection ended at " + new Date());
		return vifValues;
	}

	/**
	 * Once we got the error free list , using that list we will read the data
	 * from the input data set.
	 * 
	 * @param headersInformation
	 * @param variableSelectionDTO
	 * @param vifValues
	 * @param dataRDD
	 * @return variableSelectionDTO
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO getData(
			Map<String, Integer> headersInformation,
			VariableSelectionPropertiesDTO variableSelectionDTO,
			Map<String, Double> vifValues, JavaRDD<String> dataRDD)
			throws MRMBaseException {
		mrmDevLogger.warn("Variable selection getting data started at "
				+ new Date());
		// Based on the VIF List , we collect the data from the input RDD.
		if (null == vifValues || vifValues.isEmpty()
				|| null == variableSelectionDTO) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VIF_VALUES"));
			throw new MRMVifNotFoundException(
					MRMIndexes.getValue("EMPTY_VIF_VALUES"));
		} else {
			List<String> finalList = new ArrayList<>(vifValues.keySet());
			if (finalList.isEmpty() || null == headersInformation
					|| headersInformation.isEmpty()) {
				mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VIF_VALUES"));
				throw new MRMVifNotFoundException(
						MRMIndexes.getValue("EMPTY_VIF_VALUES"));
			} else {
				// Get column indexes and taken the data based on the indexes
				// from input file.
				List<Integer> indexValues = MRMIndexes.getColumnIndexes(
						finalList, headersInformation);
				if (null == indexValues || indexValues.isEmpty()) {
					mrmDevLogger.error(MRMIndexes.getValue("INDEX_ERROR"));
					throw new MRMColumnNotFoundException(
							MRMIndexes.getValue("INDEX_ERROR"));
				} else {
					// Reading the data from input file based on the index
					// values.
					JavaRDD<String[]> parsedData = readDataRDD(dataRDD,
							indexValues);
					if (null == parsedData || parsedData.isEmpty()) {
						mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
						throw new MRMNoDataFoundException(
								MRMIndexes.getValue("EMPTY_DATA"));
					} else {
						// Collect the data and set to DTO.
						List<String[]> listOfValues = parsedData.collect();
						variableSelectionDTO.setListOfValues(listOfValues);
					}
				}
			}
			variableSelectionDTO.setVifValuesFinalList(finalList);
		}
		mrmDevLogger.warn("Variable selection getting data ended at "
				+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method reads the input properties and set it to variable selection
	 * properties DTO.
	 * 
	 * @param propertiesValues
	 * @return variableSelectionDTO
	 * @throws MRMPropertiesNotFoundException
	 */
	private VariableSelectionPropertiesDTO readProperties(
			Map<String, String> propertiesValues)
			throws MRMPropertiesNotFoundException {
		mrmDevLogger.warn("Variable selection properties reading started at "
				+ new Date());
		VariableSelectionPropertiesDTO variableSelectionDTO = new VariableSelectionPropertiesDTO();
		// If input properties are not found then results the properties
		// exception.
		if (null == propertiesValues || propertiesValues.isEmpty()
				|| propertiesValues.size() < 6) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_PROPERTIES"));
			throw new MRMPropertiesNotFoundException(
					MRMIndexes.getValue("NO_PROPERTIES"));
		} else {
			// Read input properties from the map.
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"DEPENDENT_VAR").trim())) {
				variableSelectionDTO.setDependantVar(propertiesValues
						.get(MRMIndexes.getValue("DEPENDENT_VAR").trim())
						.toLowerCase().trim());
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"INCLUSIVE_VAR").trim())) {
				variableSelectionDTO.setInclusiveVar(propertiesValues
						.get(MRMIndexes.getValue("INCLUSIVE_VAR").toLowerCase()
								.trim()).toLowerCase().trim());
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"INDEPEN_VAR_LIST").trim())) {
				String varList = propertiesValues
						.get(MRMIndexes.getValue("INDEPEN_VAR_LIST")
								.toLowerCase().trim()).toLowerCase().trim();
				if (null != varList) {
					String[] varValuesList = varList.split(",");
					List<String> listOfVariables = new ArrayList<>();
					for (String variable : varValuesList) {
						listOfVariables.add(variable.toLowerCase().trim());
					}
					variableSelectionDTO
							.setIndependentColumnList(listOfVariables);
				}
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue(
					"SIGN_VAR_LIST").trim())) {
				String varSigns = propertiesValues
						.get(MRMIndexes.getValue("SIGN_VAR_LIST").toLowerCase()
								.trim()).toLowerCase().trim();
				if (null != varSigns) {
					String[] varValuesList = varSigns.split(",");
					List<Integer> signOfVariables = new ArrayList<>();
					for (String sign : varValuesList) {
						signOfVariables.add(Integer.parseInt(sign.trim()));
					}
					variableSelectionDTO.setSignList(signOfVariables);
				}
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue("VIF_CUTOFF")
					.trim())) {
				variableSelectionDTO.setVifCutoff(Double
						.parseDouble(propertiesValues
								.get(MRMIndexes.getValue("VIF_CUTOFF")
										.toLowerCase().trim()).toLowerCase()
								.trim()));
			}
			if (propertiesValues.containsKey(MRMIndexes.getValue("PVALUE")
					.trim())) {
				variableSelectionDTO.setpValue(Double
						.parseDouble(propertiesValues
								.get(MRMIndexes.getValue("PVALUE")
										.toLowerCase().trim()).toLowerCase()
								.trim()));
			}
		}
		mrmDevLogger.warn("Variable selection properties reading ended at "
				+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method checks the values of particular column are same or not. If
	 * all values are same then it will skip that column and will retunr the
	 * final column list.
	 * 
	 * @param dataRDD
	 * @param variableSelectionDTO
	 * @param headersInformation
	 * @return errorFreeList
	 * @throws MRMNoIndependentVariablesFoundException
	 * @throws MRMColumnNotFoundException
	 * @throws MRMNoDataFoundException
	 */
	private List<String> checkUniqueValues(JavaRDD<String> dataRDD,
			VariableSelectionPropertiesDTO variableSelectionDTO,
			Map<String, Integer> headersInformation)
			throws MRMNoIndependentVariablesFoundException,
			MRMColumnNotFoundException, MRMNoDataFoundException {
		mrmDevLogger
				.info("Variable selection checking unique values column started at "
						+ new Date());
		// Initialize the error list and taken the independent variables list.
		List<String> errorFreeList = new ArrayList<>();
		List<String> indeVariablesList = variableSelectionDTO
				.getIndependentColumnList();
		if (null == indeVariablesList || indeVariablesList.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_INDEPENDENT_LIST"));
			throw new MRMNoIndependentVariablesFoundException(
					MRMIndexes.getValue("NO_INDEPENDENT_LIST"));
		} else {
			// adding the inclusive variable value for error list because we
			// should not eliminate the this variable.
			errorFreeList.add(variableSelectionDTO.getInclusiveVar());
			for (String indeColumn : indeVariablesList) {
				// adding required columns for the regression.
				List<String> requiredColumnsList = new ArrayList<>();
				requiredColumnsList.add(variableSelectionDTO.getDependantVar());
				requiredColumnsList.add(variableSelectionDTO.getInclusiveVar());
				requiredColumnsList.add(indeColumn);
				if (requiredColumnsList.isEmpty()) {
					mrmDevLogger.warn(MRMIndexes
							.getValue("REQUIRED_COLUMN_LIST")
							+ requiredColumnsList);
				} else {
					// Taking the column indexes for particular columns.
					List<Integer> indexValues = MRMIndexes.getColumnIndexes(
							requiredColumnsList, headersInformation);

					// This method returns the columns which are not having the
					// same values.
					errorFreeList = removeUniqueValuesVariables(indexValues,
							dataRDD, requiredColumnsList, indeColumn,
							errorFreeList, variableSelectionDTO.getNoOfLines());
				}
			}
		}
		mrmDevLogger
				.info("Variable selection checking unique values column ended at "
						+ new Date());
		return errorFreeList;
	}

	/**
	 * This method checks the number of values in the column are unique or not.
	 * After checks this , it will return the columns back after removing the
	 * unique values columns.
	 * 
	 * @param indexValues
	 * @param dataRDD
	 * @param requiredColumnsList
	 * @param indeColumn
	 * @param errorFreeList
	 * @return
	 * @throws MRMColumnNotFoundException
	 * @throws MRMNoDataFoundException
	 */
	private List<String> removeUniqueValuesVariables(List<Integer> indexValues,
			JavaRDD<String> dataRDD, List<String> requiredColumnsList,
			String indeColumn, List<String> errorFreeList, int noOfLines)
			throws MRMColumnNotFoundException, MRMNoDataFoundException {
		mrmDevLogger
				.info("Variable selection checking unique values variables started at "
						+ new Date());
		if (null == indexValues || indexValues.isEmpty()
				|| indexValues.size() < 3) {
			mrmDevLogger.error(MRMIndexes.getValue("INDEX_ERROR"));
			throw new MRMColumnNotFoundException(
					MRMIndexes.getValue("INDEX_ERROR"));
		} else {
			// reading the data from the input file based on the
			// indexes.
			JavaRDD<String[]> parsedData = readDataRDD(dataRDD, indexValues);
			if (null == parsedData || parsedData.isEmpty()) {
				mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
				throw new MRMNoDataFoundException(
						MRMIndexes.getValue("EMPTY_DATA"));
			} else {
				// Checks the all the values in particular column
				// are unique or not.
				List<String[]> listOfValues = parsedData.collect();
				if (removeSameValuesColumn(listOfValues)) {
					try {
						// Check the all the values of the particular
						// column is unique or not.
						checkError(listOfValues, noOfLines, requiredColumnsList);
						errorFreeList.add(indeColumn);
					} catch (Exception exception) {
						mrmDevLogger.warn(exception.getMessage());
					}
				}
			}
		}
		mrmDevLogger
				.info("Variable selection checking unique values variables ended at "
						+ new Date());
		return errorFreeList;
	}

	/**
	 * This method check the errors when we pass the data to that. This will
	 * read data set and build the matrix. Once we build the data set and
	 * finally call the regression can find the errors.
	 * 
	 * @param listOfValues
	 * @param noOfLines
	 * @param requiredColumnsList
	 * @throws Exception
	 */
	private void checkError(List<String[]> listOfValues, int noOfLines,
			List<String> requiredColumnsList) throws Exception {
		mrmDevLogger.warn("Variable selection matrix is singular started at "
				+ new Date());
		String[][] csvMatrix = listOfValues.toArray(new String[noOfLines][]);
		int noOfColumns = requiredColumnsList.size();
		double[][] x = new double[noOfLines][noOfColumns - 1];
		double[] y = new double[noOfLines];
		if (null != csvMatrix) {
			int dCol = 0;
			for (int i = 1; i < noOfLines; i++) {
				for (int j = 0; j < noOfColumns; j++) {
					if (j < dCol && null != csvMatrix[i][j]) {
						try {
							x[i][j] = Double.parseDouble(csvMatrix[i][j]);
						} catch (Exception exception) {
							x[i][j] = MRMIndexes.parseString(csvMatrix[i][j]);
						}
					}
					if (j > dCol && null != csvMatrix[i][j]) {
						try {
							x[i][j - 1] = Double.parseDouble(csvMatrix[i][j]);
						} catch (Exception exception) {
							x[i][j - 1] = MRMIndexes
									.parseString(csvMatrix[i][j]);
						}
					}
				}
				if (null != csvMatrix[i][dCol]) {
					try {
						y[i] = Double.parseDouble(csvMatrix[i][dCol]);
					} catch (Exception exception) {
						y[i] = MRMIndexes.parseString(csvMatrix[i][dCol]);
					}
				}
			}
			try {
				LinearModel linearModel = new LinearModel(y, x);
				linearModel.getCoefficients();
			} catch (Exception exception) {
				throw exception;
			}
		}
		mrmDevLogger.warn("Variable selection matrix is singular ended at "
				+ new Date());
	}

	/**
	 * This method builds the matrix and returns the p-values and coefficient
	 * values.
	 * 
	 * @param listOfValues
	 * @param noOfLines
	 * @param requiredColumnsList
	 * @param variableSelectionDTO
	 * @return variableSelectionDTO
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO removeUnreasonableCoefficients(
			List<String[]> listOfValues, int noOfLines,
			List<String> requiredColumnsList,
			VariableSelectionPropertiesDTO variableSelectionDTO)
			throws MRMBaseException {
		mrmDevLogger
				.info("Variable selection removeUnreasonableCoefficients started at "
						+ new Date());
		if (null == listOfValues || null == requiredColumnsList) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_INDEPENDENT_LIST"));
			throw new MRMNoIndependentVariablesFoundException(
					MRMIndexes.getValue("NO_INDEPENDENT_LIST"));
		} else {
			String[][] csvMatrix = listOfValues
					.toArray(new String[noOfLines][]);
			int noOfColumns = requiredColumnsList.size();
			double[][] x = new double[noOfLines][noOfColumns - 1];
			double[] y = new double[noOfLines];

			// This sets the data from csvMatrix to x and y values.
			VariableSelectionPropertiesDTO varsDTO = readMatrixData(csvMatrix,
					x, y, noOfLines, noOfColumns, variableSelectionDTO);
			if (null != variableSelectionDTO) {
				String[] columns = new String[noOfColumns];
				for (String columnName : requiredColumnsList) {
					int indexVal = requiredColumnsList.indexOf(columnName);
					columns[indexVal] = columnName;
				}
				// This calls the regression and taking the column coefficients.
				VariableSelectionVIFCalculation olsLinearRegressionModeler = new VariableSelectionVIFCalculation();
				OLSMultipleLinearRegression linearModeler = olsLinearRegressionModeler
						.getRegressionModel(varsDTO.getY(), varsDTO.getX());
				variableSelectionDTO.setCoeffList(olsLinearRegressionModeler
						.getColumnCoefficients(linearModeler, columns));
			}
		}
		mrmDevLogger
				.info("Variable selection removeUnreasonableCoefficients ended at "
						+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method builds the matrix and returns the p-values and coefficient
	 * values.
	 * 
	 * @param listOfValues
	 * @param noOfLines
	 * @param requiredColumnsList
	 * @param variableSelectionDTO
	 * @return variableSelectionDTO
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO removeInsignificantVariables(
			List<String[]> listOfValues, int noOfLines,
			List<String> requiredColumnsList,
			VariableSelectionPropertiesDTO variableSelectionDTO)
			throws MRMNoIndependentVariablesFoundException,
			MRMLinearModelNotFoundException {
		mrmDevLogger
				.info("Variable selection removeInsignificantVariables started at "
						+ new Date());
		if (null == listOfValues || null == requiredColumnsList) {
			mrmDevLogger.error(MRMIndexes.getValue("NO_INDEPENDENT_LIST"));
			throw new MRMNoIndependentVariablesFoundException(
					MRMIndexes.getValue("NO_INDEPENDENT_LIST"));
		} else {
			String[][] csvMatrix = listOfValues
					.toArray(new String[noOfLines][]);
			int noOfColumns = requiredColumnsList.size();
			double[][] x = new double[noOfLines][noOfColumns - 1];
			double[] y = new double[noOfLines];

			// this reads the csvMatrix and sets to x and y.
			VariableSelectionPropertiesDTO varsDTO = readMatrixData(csvMatrix,
					x, y, noOfLines, noOfColumns, variableSelectionDTO);
			if (null != variableSelectionDTO) {
				String[] columns = new String[noOfColumns];
				for (String columnName : requiredColumnsList) {
					int indexVal = requiredColumnsList.indexOf(columnName);
					columns[indexVal] = columnName;
				}
				// This calls the regression on the data and taken the p-values
				// list.
				VariableSelectionVIFCalculation olsLinearRegressionModeler = new VariableSelectionVIFCalculation();
				OLSMultipleLinearRegression linearModeler = olsLinearRegressionModeler
						.getRegressionModel(varsDTO.getY(), varsDTO.getX());

				// Setting the P-values list.
				variableSelectionDTO
						.setColumnPvalues(olsLinearRegressionModeler
								.getColumnPValues(linearModeler, columns));
			}
		}
		mrmDevLogger
				.info("Variable selection removeInsignificantVariables ended at "
						+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method reads the data based on the indexes of the columns. This
	 * method reads the data line by line based on the indexes of the particular
	 * columns.
	 * 
	 * @param dataRDD
	 * @param requiredColumnIndexes
	 * @return
	 */
	private JavaRDD<String[]> readDataRDD(JavaRDD<String> dataRDD,
			final List<Integer> requiredColumnIndexes) {
		mrmDevLogger.warn("Variable selection readDataRDD started at "
				+ new Date());
		return dataRDD.map(new Function<String, String[]>() {
			private static final long serialVersionUID = 3642705290377863980L;

			@Override
			public String[] call(String line) {
				String[] lineData = line.split(",");
				String[] requiredData = null;
				if (!requiredColumnIndexes.isEmpty()) {
					requiredData = new String[requiredColumnIndexes.size()];
					for (Integer integer : requiredColumnIndexes) {
						requiredData[requiredColumnIndexes.indexOf(integer)] = lineData[integer];
					}
				}
				return requiredData;
			}
		});
	}

	/**
	 * This method checks the p-value cutoff and gives you the variables list
	 * which are values less than cutoff.
	 * 
	 * @param pvalues
	 * @param cutOff
	 * @return finalList
	 */
	private Map<String, Double> checkPvalues(Map<String, Double> pvalues,
			double cutOff) {
		mrmDevLogger.warn("Variable selection checkPvalues started at "
				+ new Date());
		Map<String, Double> finalList = new HashMap<>();
		if (null == pvalues || pvalues.isEmpty()) {
			mrmDevLogger.warn(finalList);
		} else {
			for (String column : pvalues.keySet()) {
				double pValue = pvalues.get(column);
				if (pValue < cutOff) {
					finalList.put(column, pvalues.get(column));
				}
			}
		}
		mrmDevLogger.warn("Variable selection checkPvalues ended at "
				+ new Date());
		return finalList;
	}

	/**
	 * This method removes unreasonable coefficients variables from the
	 * application.
	 * 
	 * @param finallist
	 * @param coefList
	 * @param finalLs
	 * @param variableSelectionDTO
	 * @return variableSelectionDTO
	 */
	private VariableSelectionPropertiesDTO removeUnresonableCoefficients(
			Map<String, Double> coefList,
			VariableSelectionPropertiesDTO variableSelectionDTO) {
		variableSelectionDTO.setModifiedCoefDetails(null);
		Map<String, Double> signsDetails = new HashMap<>();
		Map<String, Double> finalDetails = new HashMap<>();
		List<String> modifiedCoefList;
		Map<String, Double> modifiedCoefDetails = getVarsSignsDetails(variableSelectionDTO);
		if (null == modifiedCoefDetails || modifiedCoefDetails.isEmpty()) {
			mrmDevLogger.warn(MRMIndexes.getValue("SIGN_DETAILS_MSG"));
		} else {
			modifiedCoefList = new ArrayList<>();
			if (null == coefList || coefList.isEmpty()) {
				mrmDevLogger.warn(MRMIndexes.getValue("EMPTY_COEF_LIST"));
			} else {
				for (String coefficientColumn : coefList.keySet()) {
					double coefValue = 0.0;
					double coefficient = coefList.get(coefficientColumn);
					if (coefficientColumn.equals(variableSelectionDTO
							.getInclusiveVar())) {
						coefValue = coefficient * 0;
					} else if (coefficientColumn.equals(MRMIndexes
							.getValue("GAS_CHG"))) {
						coefValue = coefficient * 1;
					} else {
						if (null != modifiedCoefDetails.get(coefficientColumn)
								&& modifiedCoefDetails
										.containsKey(coefficientColumn)) {
							coefValue = coefficient
									* modifiedCoefDetails
											.get(coefficientColumn);
						}
					}
					signsDetails.put(coefficientColumn, coefValue);
				}
				// Checks the variables sign details.
				if (signsDetails.isEmpty()) {
					mrmDevLogger.warn(MRMIndexes.getValue("EMPTY_SIGN"));
				} else {
					// This sets the coefficient values.
					variableSelectionDTO = setCoefficientValues(
							variableSelectionDTO, signsDetails,
							modifiedCoefList, finalDetails);
				}
			}
		}
		return variableSelectionDTO;
	}

	/**
	 * This method sets the coefficient values.
	 * 
	 * @param variableSelectionDTO
	 * @param signsDetails
	 * @param modifiedCoefList
	 * @param finalDetails
	 * @return variableSelectionDTO
	 */
	private VariableSelectionPropertiesDTO setCoefficientValues(
			VariableSelectionPropertiesDTO variableSelectionDTO,
			Map<String, Double> signsDetails, List<String> modifiedCoefList,
			Map<String, Double> finalDetails) {
		mrmDevLogger.warn("Variable selection setCoefficientValues started at "
				+ new Date());
		// Taking the sign values.
		Collection<Double> values = signsDetails.values();

		// Take the minimum sign value.
		double minValue = Collections.min(values);

		// Taking the coefficient details.
		for (String columnValue : signsDetails.keySet()) {
			String trimmedColumn = columnValue.trim();
			if (null != signsDetails.get(trimmedColumn)
					&& minValue != signsDetails.get(trimmedColumn)) {
				modifiedCoefList.add(trimmedColumn);
				finalDetails
						.put(trimmedColumn, signsDetails.get(trimmedColumn));
			}
		}
		// Setting up the final coefficient details.
		variableSelectionDTO.setModifiedCoefDetails(finalDetails);
		variableSelectionDTO.setModifiedCoefList(modifiedCoefList);

		// Setting up the minimum column value.
		variableSelectionDTO.setCoefMinValue(minValue);
		mrmDevLogger.warn("Variable selection setCoefficientValues ended at "
				+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method collects the variable sign details.
	 * 
	 * @param variableSelectionDTO
	 * @return signValues
	 */
	private Map<String, Double> getVarsSignsDetails(
			VariableSelectionPropertiesDTO variableSelectionDTO) {
		mrmDevLogger.warn("Variable selection getVarsSignsDetails started at "
				+ new Date());
		Map<String, Double> signValues = new LinkedHashMap<>();
		List<String> indeVars = variableSelectionDTO.getIndependentColumnList();
		String inclusiveVar = variableSelectionDTO.getInclusiveVar();
		LinkedList<String> indeColumns = new LinkedList<>();
		indeColumns.addFirst(inclusiveVar);
		indeColumns.addAll(1, indeVars);
		List<Integer> signList = variableSelectionDTO.getSignList();
		if (null == signList || indeColumns.isEmpty() || signList.isEmpty()
				|| indeColumns.size() != signList.size()) {
			mrmDevLogger.warn(MRMIndexes.getValue("SIGN_DETAILS_MSG"));
		} else {
			for (String integer : indeColumns) {
				int index = indeColumns.indexOf(integer);
				signValues.put(integer, (double) signList.get(index));
			}
		}
		mrmDevLogger.warn("Variable selection getVarsSignsDetails ended at "
				+ new Date());
		return signValues;
	}

	/**
	 * This method will return the VIF Values based on the independent variables
	 * list.
	 * 
	 * @param dataRDD
	 * @param vifCalculationVarList
	 * @param cutOff
	 * @param inclusiveVar
	 * @throws MRMBaseException
	 */
	private Map<String, Double> calculateVIF(JavaRDD<String> dataRDD,
			List<String> vifCalculationVarList, int cutOff, String inclusiveVar)
			throws MRMBaseException {
		VariableSelectionVIFCalculation olsLinearRegressionModeler = new VariableSelectionVIFCalculation();
		return olsLinearRegressionModeler.getVIFValues(dataRDD,
				vifCalculationVarList, cutOff, inclusiveVar);
	}

	/**
	 * This method remove variables with unreasonable coefficients.
	 * 
	 * @param listOfValues
	 * @param noOfLines
	 * @param finalList
	 * @param variableSelectionDTO
	 * @return variableSelectionDetails
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO getUnresonableCoefficientList(
			List<String[]> listOfValues, int noOfLines, List<String> finalList,
			VariableSelectionPropertiesDTO variableSelectionDTO)
			throws MRMBaseException {
		VariableSelectionPropertiesDTO variableSelectionDetails = null;
		if (null == listOfValues || null == finalList || finalList.isEmpty()
				|| listOfValues.isEmpty()) {
			mrmDevLogger.warn(MRMIndexes.getValue("INVALID_INPUT_COEFF"));
		} else {
			// calls remove unreasonable coefficients method
			variableSelectionDetails = removeUnreasonableCoefficients(
					listOfValues, noOfLines, finalList, variableSelectionDTO);

			// Run the regression and collect the coefficient list.
			Map<String, Double> coefList = variableSelectionDetails
					.getCoeffList();
			if (null == coefList || coefList.isEmpty()) {
				mrmDevLogger
						.error(MRMIndexes.getValue("MODIFIED_COEF_DETAILS"));
				throw new MRMCoefficientsNotFoundException(
						MRMIndexes.getValue("MODIFIED_COEF_DETAILS"));
			} else {
				// Run the regression and collect the coefficient list.
				variableSelectionDetails = removeUnresonableCoefficients(
						coefList, variableSelectionDTO);

				// Checking the minimum coefficient value.
				while (variableSelectionDetails.getCoefMinValue() < 0) {
					variableSelectionDetails = removeUnreasonableCoefficients(
							listOfValues, noOfLines,
							variableSelectionDetails.getModifiedCoefList(),
							variableSelectionDTO);

					// Again checking the modified coefficient details.
					if (null != variableSelectionDetails
							.getModifiedCoefDetails()
							&& !variableSelectionDetails
									.getModifiedCoefDetails().isEmpty()) {
						variableSelectionDetails = removeUnresonableCoefficients(
								variableSelectionDetails.getCoeffList(),
								variableSelectionDTO);
					}
				}
			}
		}
		return variableSelectionDetails;
	}

	/**
	 * This method takes the independent variables column values and check the
	 * values are same in the column. If we get same values in the column, then
	 * will get matrix is singular problem.
	 * 
	 * @param listOfValues
	 * @param indeColumn
	 * @return independentColumnValues.size() > 1 ? true : false
	 */
	public boolean removeSameValuesColumn(List<String[]> listOfValues) {
		Set<String> independentColumnValues = new HashSet<>();
		if (null == listOfValues || listOfValues.isEmpty()) {
			mrmDevLogger.warn(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			for (String[] columnValues : listOfValues) {
				if (null != columnValues && columnValues.length > 1) {
					int value = listOfValues.indexOf(columnValues);
					if (value > 0) {
						independentColumnValues.add(columnValues[2]);
					}
				}
			}
		}
		return independentColumnValues.size() > 1 ? true : false;
	}

	/**
	 * This class reads the data from the input file while taking the
	 * coefficients or p-values.
	 * 
	 * @param csvMatrix
	 * @param independentMatrix
	 * @param dependentMatrix
	 * @param noOfLines
	 * @param noOfColumns
	 * @param variableSelectionDTO
	 */
	private VariableSelectionPropertiesDTO readMatrixData(String[][] csvMatrix,
			double[][] independentMatrix, double[] dependentMatrix,
			int noOfLines, int noOfColumns,
			VariableSelectionPropertiesDTO variableSelectionDTO) {
		if (null == variableSelectionDTO || null == csvMatrix
				|| null == independentMatrix || null == dependentMatrix) {
			mrmDevLogger.warn(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			int columnIndex = 0;
			for (int rowNumber = 1; rowNumber < noOfLines; rowNumber++) {
				for (int columnNumber = 0; columnNumber < noOfColumns; columnNumber++) {
					if (columnNumber < columnIndex
							&& null != csvMatrix[rowNumber][columnNumber]) {
						try {
							independentMatrix[rowNumber][columnNumber] = Double
									.parseDouble(csvMatrix[rowNumber][columnNumber]);
						} catch (NumberFormatException e) {
							independentMatrix[rowNumber][columnNumber] = MRMIndexes
									.parseString(csvMatrix[rowNumber][columnNumber]);
						}
					}
					if (columnNumber > columnIndex
							&& null != csvMatrix[rowNumber][columnNumber]) {
						try {
							independentMatrix[rowNumber][columnNumber - 1] = Double
									.parseDouble(csvMatrix[rowNumber][columnNumber]);
						} catch (NumberFormatException e) {
							independentMatrix[rowNumber][columnNumber - 1] = MRMIndexes
									.parseString(csvMatrix[rowNumber][columnNumber]);
						}
					}
				}
				if (null != csvMatrix[rowNumber][columnIndex]) {
					try {
						dependentMatrix[rowNumber] = Double
								.parseDouble(csvMatrix[rowNumber][columnIndex]);
					} catch (NumberFormatException e) {
						dependentMatrix[rowNumber] = MRMIndexes
								.parseString(csvMatrix[rowNumber][columnIndex]);
					}
				}
			}
			variableSelectionDTO.setX(independentMatrix);
			variableSelectionDTO.setY(dependentMatrix);
		}
		return variableSelectionDTO;
	}

	/**
	 * This method gives you the p-values list for the variables list and
	 * removes the insignificant variables from the variables list.
	 * 
	 * @param variableSelectionDTO
	 * @return variableSelectionDTO
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO getPvaluesList(
			VariableSelectionPropertiesDTO variableSelectionDTO)
			throws MRMPValuesNotFoundException,
			MRMNoIndependentVariablesFoundException,
			MRMLinearModelNotFoundException {
		if (null == variableSelectionDTO
				|| null == variableSelectionDTO.getCoeffList()
				|| variableSelectionDTO.getCoeffList().isEmpty()) {
			mrmDevLogger.warn(MRMIndexes.getValue("EMPTY_PVALUE"));
		} else {
			List<String> pvaluesInputList = new ArrayList<>(
					variableSelectionDTO.getCoeffList().keySet());
			if (pvaluesInputList.isEmpty()) {
				mrmDevLogger.error(MRMIndexes.getValue("EMPTY_PVALUE"));
				throw new MRMPValuesNotFoundException(
						MRMIndexes.getValue("EMPTY_PVALUE"));
			} else {
				// this removes the insignificant variables.
				variableSelectionDTO = removeInsignificantVariables(
						variableSelectionDTO.getListOfValues(),
						variableSelectionDTO.getNoOfLines(), pvaluesInputList,
						variableSelectionDTO);
				Map<String, Double> pvalues = variableSelectionDTO
						.getColumnPvalues();
				if (null == pvalues || pvalues.isEmpty()) {
					mrmDevLogger.error(MRMIndexes.getValue("EMPTY_PVALUE"));
					throw new MRMPValuesNotFoundException(
							MRMIndexes.getValue("EMPTY_PVALUE"));
				} else {
					// this call checks the p-values whether this p-value is
					// less than the threshold value or not.
					Map<String, Double> vifValues = checkPvalues(pvalues,
							variableSelectionDTO.getpValue());
					// Set final variables after remove insignificant variables.
					variableSelectionDTO.setVifValues(vifValues);
				}
			}
		}
		return variableSelectionDTO;
	}

	/**
	 * This method calculates the VIF Values of the error free list.
	 * 
	 * @param variableSelectionDTO
	 * @param dataRDD
	 * @param cutOff
	 * @return variableSelectionDTO
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO calculateVifValues(
			VariableSelectionPropertiesDTO variableSelectionDTO,
			JavaRDD<String> dataRDD, int cutOff) throws MRMBaseException {
		mrmDevLogger.warn("Variable selection calculate vif values started at "
				+ new Date());
		// we calculates the VIF values for error free list.
		if (null == variableSelectionDTO
				|| null == variableSelectionDTO.getErrorFreeList()
				|| variableSelectionDTO.getErrorFreeList().isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("ERROR_FREE_LIST_MSG"));
			throw new MRMVariablesNotFoundException(
					MRMIndexes.getValue("ERROR_FREE_LIST_MSG"));
		} else {
			// Here, calculates the VIF Values of the variables.
			Map<String, Double> vifValues = calculateVIF(dataRDD,
					variableSelectionDTO.getErrorFreeList(), cutOff,
					variableSelectionDTO.getInclusiveVar());

			// Once return the list, we can get the data.
			variableSelectionDTO = getData(
					variableSelectionDTO.getHeadersInformation(),
					variableSelectionDTO, vifValues, dataRDD);
			variableSelectionDTO.setVifValues(vifValues);
		}
		mrmDevLogger.warn("Variable selection calculate vif values ended at "
				+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method removes the unreasonable coefficients.
	 * 
	 * @param variableSelectionDTO
	 * @return
	 * @throws MRMBaseException
	 */
	private VariableSelectionPropertiesDTO removeUnresonableCoefList(
			VariableSelectionPropertiesDTO variableSelectionDTO)
			throws MRMBaseException {
		mrmDevLogger
				.info("Variable selection remove unreasonable coefficient list started at "
						+ new Date());
		// We remove variables with unreasonable coefficients
		if (null == variableSelectionDTO
				|| null == variableSelectionDTO.getListOfValues()
				|| variableSelectionDTO.getListOfValues().isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			// remove variables with unreasonable coefficients.
			variableSelectionDTO = getUnresonableCoefficientList(
					variableSelectionDTO.getListOfValues(),
					variableSelectionDTO.getNoOfLines(),
					variableSelectionDTO.getVifValuesFinalList(),
					variableSelectionDTO);
		}
		mrmDevLogger
				.info("Variable selection remove unreasonable coefficient list ended at "
						+ new Date());
		return variableSelectionDTO;
	}

	/**
	 * This method remove insignificant variables.
	 * 
	 * @param variableSelectionDTO
	 * @return variableSelectionDTO
	 * @throws MRMPValuesNotFoundException
	 * @throws MRMNoIndependentVariablesFoundException
	 * @throws MRMLinearModelNotFoundException
	 */
	private VariableSelectionPropertiesDTO removeInsignificantVars(
			VariableSelectionPropertiesDTO variableSelectionDTO)
			throws MRMPValuesNotFoundException,
			MRMNoIndependentVariablesFoundException,
			MRMLinearModelNotFoundException {
		mrmDevLogger
				.info("Variable selection remove insignificant variables started at "
						+ new Date());
		// We remove insignificant variables based on p-value
		if (null == variableSelectionDTO.getCoeffList()
				|| variableSelectionDTO.getCoeffList().isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_PVALUE"));
			throw new MRMPValuesNotFoundException(
					MRMIndexes.getValue("EMPTY_PVALUE"));
		} else {
			// Get p-values list and set the final p-values list.
			variableSelectionDTO = getPvaluesList(variableSelectionDTO);
			Map<String, Double> vifValues = variableSelectionDTO.getVifValues();
			variableSelectionDTO.setVifValues(vifValues);
		}
		mrmDevLogger
				.info("Variable selection remove insignificant variables started at "
						+ new Date());
		return variableSelectionDTO;
	}

}
