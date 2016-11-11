/**
 * 
 */
package variableSelection;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.util.FastMath;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import regression.LinearRegressionModelDTO;
import util.MRMIndexes;
import exceptions.MRMBaseException;
import exceptions.MRMHeaderNotFoundException;
import exceptions.MRMLinearModelNotFoundException;
import exceptions.MRMNoDataFoundException;
import exceptions.MRMVariablesNotFoundException;
import exceptions.MRMVifNotFoundException;

/**
 * This class contains the methods which will calculates VIF Values for
 * variables in variable selection module.
 * 
 * @author Naresh
 *
 */
public class VariableSelectionVIFCalculation implements Serializable {

	private static final long serialVersionUID = 7799249838446135049L;

	static final Logger mrmDevLogger = Logger
			.getLogger(VariableSelectionVIFCalculation.class);

	/**
	 * This method gives you VIF Values based on the variables list.
	 * 
	 * @param dataRDD
	 * @param varList
	 * @param cutOff
	 * @param inclusiveVar
	 * @return maxVifValues
	 * @throws MRMBaseException
	 */
	public Map<String, Double> getVIFValues(JavaRDD<String> dataRDD,
			List<String> varList, int cutOff, String inclusiveVar)
			throws MRMBaseException {
		LinearRegressionModelDTO linearRegressionModelDTO = new LinearRegressionModelDTO();
		Map<String, Double> maxVifValues;
		if (null == varList || varList.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VAR_LIST"));
			throw new MRMVariablesNotFoundException(
					MRMIndexes.getValue("EMPTY_VAR_LIST"));
		} else {
			// Get all variables VIF Values.
			linearRegressionModelDTO = getAllVarsVIF(dataRDD,
					linearRegressionModelDTO, varList);
			if (null == linearRegressionModelDTO.getMaxVifValues()) {
				mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VIF_VALUE"));
				throw new MRMVifNotFoundException(
						MRMIndexes.getValue("EMPTY_VIF_VALUE"));
			} else {
				// Find out the maximum VIF Values.
				maxVifValues = linearRegressionModelDTO.getMaxVifValues();
				List<String> excludeColumnsList = new ArrayList<>();
				linearRegressionModelDTO
						.setExcludeColumnsList(excludeColumnsList);
				if (null == maxVifValues || maxVifValues.isEmpty()) {
					mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VIF_VALUE"));
					throw new MRMVifNotFoundException(
							MRMIndexes.getValue("EMPTY_VIF_VALUE"));
				} else {
					if (maxVifValues.containsKey(inclusiveVar)) {
						double inclusivVarVIF = maxVifValues.get(inclusiveVar);
						if (inclusivVarVIF > cutOff) {
							// Checking the inclusive variable VIF Value.
							linearRegressionModelDTO = checkInclusiveVarVIF(
									maxVifValues, inclusiveVar,
									linearRegressionModelDTO, varList, cutOff,
									dataRDD);
							maxVifValues = linearRegressionModelDTO
									.getMaxVifValues();
							// Checks the VIF Values.
							linearRegressionModelDTO = checkVIFVals(
									maxVifValues, linearRegressionModelDTO,
									varList, dataRDD);
							maxVifValues = linearRegressionModelDTO
									.getMaxVifValues();
						} else {
							List<String> finalList = new ArrayList<>();
							if (varList.isEmpty()) {
								mrmDevLogger.info(maxVifValues);
							} else {
								for (String finalColumn : varList) {
									if (!excludeColumnsList
											.contains(finalColumn)) {
										finalList.add(finalColumn);
									}
								}
								// Call the all variables VIF Values.
								linearRegressionModelDTO = getAllVarsVIF(
										dataRDD, linearRegressionModelDTO,
										varList);
								maxVifValues = linearRegressionModelDTO
										.getMaxVifValues();
							}
						}
					}
				}
			}
		}
		return maxVifValues;
	}

	/**
	 * This method checks the inclusive variables VIF Value. If the inclusive
	 * variables value is greater than threshold value, then take the maximum
	 * value from the list and exclude from the list and run again regression
	 * and take the VIF Values. This process will continue till VIF Values of
	 * the variables less than the threshold value.
	 * 
	 * @param maxVifValues
	 * @param inclusiveVar
	 * @param linearRegressionModelDTO
	 * @param varList
	 * @param cutOff
	 * @param dataRDD
	 * @throws MRMHeaderNotFoundException
	 *             , {@link MRMVifNotFoundException}
	 */
	private LinearRegressionModelDTO checkInclusiveVarVIF(
			Map<String, Double> maxVifValues, String inclusiveVar,
			LinearRegressionModelDTO linearRegressionModelDTO,
			List<String> varList, int cutOff, JavaRDD<String> dataRDD)
			throws MRMHeaderNotFoundException, MRMVifNotFoundException,
			MRMLinearModelNotFoundException {
		if (null == linearRegressionModelDTO || null == maxVifValues
				|| maxVifValues.isEmpty()) {
			mrmDevLogger.info(maxVifValues);
		} else {
			// Checks the Maximum VIF Values empty condition.
			while (!maxVifValues.isEmpty()) {
				if (maxVifValues.containsKey(inclusiveVar)) {
					double inclusivVarVIF = maxVifValues.get(inclusiveVar);
					if (inclusivVarVIF < cutOff)
						break;
					maxVifValues.remove(inclusiveVar);
				}

				// Checks the VIF Values range condition.
				if (checkVifValuesRange(maxVifValues, cutOff))
					break;

				// Get exclude columns list.
				List<String> excludeColumnsList = getExcludeList(
						linearRegressionModelDTO.getExcludeColumnsList(),
						maxVifValues, inclusiveVar);
				if (!excludeColumnsList.isEmpty()) {
					linearRegressionModelDTO
							.setExcludeColumnsList(excludeColumnsList);
					linearRegressionModelDTO = getHighVIFValue(dataRDD,
							linearRegressionModelDTO, varList, cutOff);
					maxVifValues = linearRegressionModelDTO.getMaxVifValues();
				}
			}
			linearRegressionModelDTO.setMaxVifValues(maxVifValues);
		}
		return linearRegressionModelDTO;
	}

	/**
	 * This method returns the final variables list back.
	 * 
	 * @param maxVifValues
	 * @param linearRegressionModelDTO
	 * @param varList
	 * @param cutOff
	 * @param dataRDD
	 * @return linearRegressionModelDTO
	 * @throws MRMHeaderNotFoundException
	 * @throws MRMLinearModelNotFoundException
	 * @throws MRMVariablesNotFoundException
	 */
	private LinearRegressionModelDTO checkVIFVals(
			Map<String, Double> maxVifValues,
			LinearRegressionModelDTO linearRegressionModelDTO,
			List<String> varList, JavaRDD<String> dataRDD)
			throws MRMHeaderNotFoundException, MRMLinearModelNotFoundException,
			MRMVariablesNotFoundException {
		if (maxVifValues.isEmpty()) {
			List<String> finalList = new ArrayList<>();
			if (null == varList || varList.isEmpty()
					|| null == linearRegressionModelDTO) {
				mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VAR_LIST"));
				throw new MRMVariablesNotFoundException(
						MRMIndexes.getValue("EMPTY_VAR_LIST"));
			} else {
				for (String finalColumn : varList) {
					if (!linearRegressionModelDTO.getExcludeColumnsList()
							.contains(finalColumn)) {
						finalList.add(finalColumn);
					}
				}
				if (finalList.isEmpty()) {
					mrmDevLogger.info(MRMIndexes.getValue("EMPTY_LIST"));
				} else {
					linearRegressionModelDTO = getAllVarsVIF(dataRDD,
							linearRegressionModelDTO, finalList);
					maxVifValues = linearRegressionModelDTO.getMaxVifValues();
					linearRegressionModelDTO.setMaxVifValues(maxVifValues);
				}
			}
		}
		return linearRegressionModelDTO;
	}

	/**
	 * This method takes the variables header information and will take the data
	 * based on the indexes.
	 * 
	 * @param dataRDD
	 * @param linearRegressionModelDTO
	 * @param varList
	 * @param cutOff
	 * @return linearRegressionModelDTO
	 * @throws MRMHeaderNotFoundException
	 * @throws MRMLinearModelNotFoundException
	 */
	private LinearRegressionModelDTO getHighVIFValue(JavaRDD<String> dataRDD,
			LinearRegressionModelDTO linearRegressionModelDTO,
			List<String> varList, int cutOff)
			throws MRMHeaderNotFoundException, MRMLinearModelNotFoundException {
		JavaRDD<String[]> parsedData = null;
		List<Integer> indexValues;

		// Checks the header information.
		Map<String, Integer> headersInformation = MRMIndexes
				.getHeadersInformation(dataRDD);
		if (null == headersInformation || headersInformation.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("COLUMN_HEADER_WARN"));
			throw new MRMHeaderNotFoundException(
					MRMIndexes.getValue("COLUMN_HEADER_WARN"));
		} else {
			if (null != linearRegressionModelDTO.getExcludeColumnsList()
					&& !linearRegressionModelDTO.getExcludeColumnsList()
							.isEmpty()) {
				indexValues = excludeColumns(varList, headersInformation,
						linearRegressionModelDTO.getExcludeColumnsList());
				if (!indexValues.isEmpty())
					parsedData = readDataRDD(dataRDD, indexValues);
			} else {
				// Get the column indexes.
				indexValues = MRMIndexes.getColumnIndexes(varList,
						headersInformation);
				if (!indexValues.isEmpty())
					// Reading the data based on the index values.
					parsedData = readDataRDD(dataRDD, indexValues);
			}
			try {
				if (null != parsedData) {
					Map<String, Double> values = getMaxVIFValues(parsedData,
							indexValues.size(), cutOff);
					linearRegressionModelDTO.setMaxVifValues(values);
				}
			} catch (Exception exception) {
				mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
				throw new MRMLinearModelNotFoundException(
						MRMIndexes.getValue("EXC_MSG"));
			}
		}
		return linearRegressionModelDTO;
	}

	/**
	 * This method collect the VIF Values for the all variables. Here, this
	 * method will not check with the threshold value.
	 * 
	 * @param dataRDD
	 * @param linearRegressionModelDTO
	 * @param varList
	 * @return
	 * @throws MRMHeaderNotFoundException
	 * @throws MRMLinearModelNotFoundException
	 */
	private LinearRegressionModelDTO getAllVarsVIF(JavaRDD<String> dataRDD,
			LinearRegressionModelDTO linearRegressionModelDTO,
			List<String> varList) throws MRMHeaderNotFoundException,
			MRMLinearModelNotFoundException {
		JavaRDD<String[]> parsedData = null;
		List<Integer> indexValues;

		// Checks the headers information.
		Map<String, Integer> headersInformation = MRMIndexes
				.getHeadersInformation(dataRDD);
		if (null == headersInformation || headersInformation.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("COLUMN_HEADER_WARN"));
			throw new MRMHeaderNotFoundException(
					MRMIndexes.getValue("COLUMN_HEADER_WARN"));
		} else {
			if (null != linearRegressionModelDTO.getExcludeColumnsList()
					&& !linearRegressionModelDTO.getExcludeColumnsList()
							.isEmpty()) {
				indexValues = excludeColumns(varList, headersInformation,
						linearRegressionModelDTO.getExcludeColumnsList());
				if (!indexValues.isEmpty())
					parsedData = readDataRDD(dataRDD, indexValues);
			} else {
				// Get the respective column indexes.
				indexValues = MRMIndexes.getColumnIndexes(varList,
						headersInformation);
				if (!indexValues.isEmpty())

					// Read the data based on the indexes.
					parsedData = readDataRDD(dataRDD, indexValues);
			}
			try {
				if (null != parsedData) {
					Map<String, Double> values = getAllVarsVIFValues(
							parsedData, indexValues.size());
					linearRegressionModelDTO.setMaxVifValues(values);
				}
			} catch (Exception exception) {
				mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
				throw new MRMLinearModelNotFoundException(
						MRMIndexes.getValue("EXC_MSG"));
			}
		}
		return linearRegressionModelDTO;
	}

	/**
	 * This method returns the maximum high VIF Values. Here , this will check
	 * the threshold value.
	 * 
	 * @param parsedData
	 * @param numberOfColumns
	 * @return vifValues
	 */
	private Map<String, Double> getMaxVIFValues(JavaRDD<String[]> parsedData,
			int numberOfColumns, int cutOff)
			throws MRMLinearModelNotFoundException, MRMNoDataFoundException {
		Map<String, Double> vifValues = new HashMap<>();
		if (null == parsedData || parsedData.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			String[] headerRow = parsedData.first();
			parsedData.cache();
			final int noOfLines = (int) parsedData.count();
			List<String[]> listOfValues = parsedData.collect();
			if (null != listOfValues && !listOfValues.isEmpty()) {
				String[][] csvMatrix = listOfValues
						.toArray(new String[noOfLines][]);
				double[][] x = new double[noOfLines][numberOfColumns - 1];
				double[] y = new double[noOfLines];
				if (null != csvMatrix) {
					for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
						for (int rowNumber = 1; rowNumber < noOfLines; rowNumber++) {
							for (int columnNumber = 0; columnNumber < numberOfColumns; columnNumber++) {
								if (columnNumber < columnIndex
										&& null != csvMatrix[rowNumber][columnNumber]) {
									try {
										x[rowNumber][columnNumber] = Double
												.parseDouble(csvMatrix[rowNumber][columnNumber]);
									} catch (NumberFormatException e) {
										x[rowNumber][columnNumber] = MRMIndexes
												.parseString(csvMatrix[rowNumber][columnNumber]);
									}
								}
								if (columnNumber > columnIndex
										&& null != csvMatrix[rowNumber][columnNumber]) {
									try {
										x[rowNumber][columnNumber - 1] = Double
												.parseDouble(csvMatrix[rowNumber][columnNumber]);
									} catch (NumberFormatException e) {
										x[rowNumber][columnNumber - 1] = MRMIndexes
												.parseString(csvMatrix[rowNumber][columnNumber]);
									}
								}

							}
							if (null != csvMatrix[rowNumber][columnIndex]) {
								try {
									y[rowNumber] = Double
											.parseDouble(csvMatrix[rowNumber][columnIndex]);
								} catch (NumberFormatException e) {
									y[rowNumber] = MRMIndexes
											.parseString(csvMatrix[rowNumber][columnIndex]);
								}
							}
						}
						try {
							vifValues = getLRModel(y, x, columnIndex,
									headerRow, cutOff, vifValues);
						} catch (Exception exception) {
							throw new MRMLinearModelNotFoundException(
									MRMIndexes.getValue("EXC_MSG"));
						}
					}
				}
			}
		}
		return vifValues;
	}

	/**
	 * This method collects VIF Values for all the variables , this will not
	 * check the threshold condition.
	 * 
	 * @param parsedData
	 * @param numberOfColumns
	 * @return vifValues
	 * @throws MRMLinearModelNotFoundException
	 * @throws MRMNoDataFoundException
	 */
	private Map<String, Double> getAllVarsVIFValues(
			JavaRDD<String[]> parsedData, int numberOfColumns)
			throws MRMLinearModelNotFoundException, MRMNoDataFoundException {
		Map<String, Double> vifValues = new HashMap<>();
		if (null == parsedData || parsedData.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_DATA"));
			throw new MRMNoDataFoundException(MRMIndexes.getValue("EMPTY_DATA"));
		} else {
			String[] headerRow = parsedData.first();
			parsedData.cache();
			final int noOfLines = (int) parsedData.count();
			List<String[]> listOfValues = parsedData.collect();
			if (null != listOfValues && !listOfValues.isEmpty()) {
				String[][] csvMatrix = listOfValues
						.toArray(new String[noOfLines][]);
				double[][] x = new double[noOfLines][numberOfColumns - 1];
				double[] y = new double[noOfLines];
				if (null != csvMatrix) {
					for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
						for (int rowNumber = 1; rowNumber < noOfLines; rowNumber++) {
							for (int columnNumber = 0; columnNumber < numberOfColumns; columnNumber++) {
								if (columnNumber < columnIndex
										&& null != csvMatrix[rowNumber][columnNumber]) {
									try {
										x[rowNumber][columnNumber] = Double
												.parseDouble(csvMatrix[rowNumber][columnNumber]);
									} catch (NumberFormatException e) {
										x[rowNumber][columnNumber] = MRMIndexes
												.parseString(csvMatrix[rowNumber][columnNumber]);
									}
								}
								if (columnNumber > columnIndex
										&& null != csvMatrix[rowNumber][columnNumber]) {
									try {
										x[rowNumber][columnNumber - 1] = Double
												.parseDouble(csvMatrix[rowNumber][columnNumber]);
									} catch (NumberFormatException e) {
										x[rowNumber][columnNumber - 1] = MRMIndexes
												.parseString(csvMatrix[rowNumber][columnNumber]);
									}
								}
							}
							if (null != csvMatrix[rowNumber][columnIndex]) {
								try {
									y[rowNumber] = Double
											.parseDouble(csvMatrix[rowNumber][columnIndex]);
								} catch (NumberFormatException e) {
									y[rowNumber] = MRMIndexes
											.parseString(csvMatrix[rowNumber][columnIndex]);
								}
							}
						}
						try {
							vifValues = getAllVarsLRModel(y, x, columnIndex,
									headerRow, vifValues);
						} catch (Exception exception) {
							mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
							throw new MRMLinearModelNotFoundException(
									MRMIndexes.getValue("EXC_MSG"));
						}
					}
				}
			}
		}
		return vifValues;
	}

	/**
	 * This method reads the RDD and will give you the array of values in RDD.
	 * 
	 * @param dataRDD
	 * @param requiredColumnIndexes
	 * @param headersInformation
	 */
	private JavaRDD<String[]> readDataRDD(JavaRDD<String> dataRDD,
			final List<Integer> requiredColumnIndexes) {
		return dataRDD.map(new Function<String, String[]>() {
			private static final long serialVersionUID = 3642705290377863980L;

			@Override
			public String[] call(String line) {
				String[] lineData = line.split(",");
				String[] requiredData = null;
				if (!requiredColumnIndexes.isEmpty()) {
					requiredData = new String[requiredColumnIndexes.size()];
					for (Integer integer : requiredColumnIndexes) {
						int indexValue = requiredColumnIndexes.indexOf(integer);
						requiredData[indexValue] = lineData[integer];
					}
				}
				return requiredData;
			}
		});
	}

	/**
	 * This method will give the list of column names and vif values which are
	 * more than threshold value.
	 * 
	 * @param dependentMatrix
	 * @param independentMatrix
	 * @param dCol
	 * @param columnNames
	 * @param cutOff
	 * @param vifValues
	 * @return vifValues
	 * @throws MRMLinearModelNotFoundException
	 */
	private Map<String, Double> getLRModel(double[] dependentMatrix,
			double[][] independentMatrix, int dCol, String[] columnNames,
			int cutOff, Map<String, Double> vifValues)
			throws MRMLinearModelNotFoundException {
		try {
			// Call the linear model.
			OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
			regression.newSampleData(dependentMatrix, independentMatrix);

			// Calculate rsquare and VIF.
			double rSquareVal = regression.calculateRSquared();
			double vif = 1 / (1 - rSquareVal);

			// Check the VIF less than threshold value.
			if (vif > cutOff && null != columnNames[dCol]) {
				vifValues.put(columnNames[dCol].toLowerCase(), vif);
			}
		} catch (Exception exception) {
			throw new MRMLinearModelNotFoundException(
					MRMIndexes.getValue("EXC_MSG"));
		}
		return vifValues;
	}

	/**
	 * This method will give the list of column names and vif values which are
	 * more than threshold value.
	 * 
	 * @param dependentMatrix
	 * @param independentMatrix
	 * @param columnIndex
	 * @param columnNames
	 * @param vifValues
	 * @return vifValues
	 * @throws MRMLinearModelNotFoundException
	 */
	private Map<String, Double> getAllVarsLRModel(double[] dependentMatrix,
			double[][] independentMatrix, int columnIndex,
			String[] columnNames, Map<String, Double> vifValues)
			throws MRMLinearModelNotFoundException {
		try {
			// Call the linear model.
			OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
			regression.newSampleData(dependentMatrix, independentMatrix);

			// Calculate rsquare and VIF.
			double rSquareVal = regression.calculateRSquared();
			double vif = 1 / (1 - rSquareVal);

			// Check the VIF less than threshold value.
			if (null != columnNames[columnIndex]) {
				vifValues.put(columnNames[columnIndex].toLowerCase(), vif);
			}
		} catch (Exception exception) {
			mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
			throw new MRMLinearModelNotFoundException(
					MRMIndexes.getValue("EXC_MSG"));
		}
		return vifValues;
	}

	/**
	 * This method exclude columns which are having VIF value more than
	 * threshold value.
	 * 
	 * @param requiredColumnNames
	 * @param headersInformation
	 * @param excludeColumnList
	 * @return
	 * @throws MRMHeaderNotFoundException
	 */
	private List<Integer> excludeColumns(List<String> requiredColumnNames,
			Map<String, Integer> headersInformation,
			List<String> excludeColumnList) throws MRMHeaderNotFoundException {
		List<Integer> requiredData = new ArrayList<>();
		for (String columnName : requiredColumnNames) {
			String columnValue = columnName.trim();
			if (headersInformation.containsKey(columnValue)
					&& null != excludeColumnList
					&& !excludeColumnList.contains(columnValue)) {
				requiredData.add(headersInformation.get(columnValue));
			}
		}
		return requiredData;
	}

	/**
	 * This method returns the p-values based on the regression model.
	 * 
	 * @param regression
	 * @return pvalues
	 * @throws MRMLinearModelNotFoundException
	 */
	public List<Double> getPValues(OLSMultipleLinearRegression regression)
			throws MRMLinearModelNotFoundException {
		List<Double> pvalues = new ArrayList<>();
		if (null == regression) {
			mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
			throw new MRMLinearModelNotFoundException(
					MRMIndexes.getValue("EXC_MSG"));
		} else {
			// Getting the regression Parameters.
			double[] beta = regression.estimateRegressionParameters();

			// Taking the standard Errors.
			double[] standardErrors = new double[beta.length];
			try {
				standardErrors = regression
						.estimateRegressionParametersStandardErrors();
			} catch (Exception exception) {
				mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
				throw new MRMLinearModelNotFoundException(
						MRMIndexes.getValue("EXC_MSG"));
			}

			// Taking residual distribution.
			int residualdf = regression.estimateResiduals().length
					- beta.length;

			// Calculating the p-values.
			for (int i = 0; i < beta.length; i++) {
				// Finding the tstat value.
				double tstat = beta[i] / standardErrors[i];

				// Calculate the pvalue.
				double pvalue = new TDistribution(residualdf)
						.cumulativeProbability(-FastMath.abs(tstat)) * 2;
				pvalues.add(pvalue);
			}
		}
		return pvalues;
	}

	/**
	 * This method returns the columns and corresponding p-values based on the
	 * regression.
	 * 
	 * @param regression
	 * @param columnNames
	 * @return pvalues
	 * @throws MRMLinearModelNotFoundException
	 */
	public Map<String, Double> getColumnPValues(
			OLSMultipleLinearRegression regression, String[] columnNames)
			throws MRMLinearModelNotFoundException {
		Map<String, Double> pvalues = new HashMap<>();
		if (null == regression) {
			mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
			throw new MRMLinearModelNotFoundException(
					MRMIndexes.getValue("EXC_MSG"));
		} else {
			// Getting the regression Parameters.
			double[] beta = regression.estimateRegressionParameters();

			// Taking the standard Errors.
			double[] standardErrors = new double[beta.length];
			try {
				standardErrors = regression
						.estimateRegressionParametersStandardErrors();
			} catch (Exception exception) {
				mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
				throw new MRMLinearModelNotFoundException(
						MRMIndexes.getValue("EXC_MSG"));
			}

			// Taking residual distribution.
			int residualdf = regression.estimateResiduals().length
					- beta.length;

			// Calculating the p-values.
			for (int betaValue = 0; betaValue < beta.length; betaValue++) {
				// Finding the tstat value.
				double tstat = beta[betaValue] / standardErrors[betaValue];

				// Calculate the pvalue.
				double pvalue = new TDistribution(residualdf)
						.cumulativeProbability(-FastMath.abs(tstat)) * 2;
				pvalues.put(columnNames[betaValue], pvalue);
			}
		}
		return pvalues;
	}

	/**
	 * This method returns the coefficients of the model.
	 * 
	 * @param regression
	 * @return coefficients
	 * @throws MRMLinearModelNotFoundException
	 */
	public List<Double> getCoefficients(OLSMultipleLinearRegression regression)
			throws MRMLinearModelNotFoundException {
		List<Double> coefficients = new ArrayList<>();
		if (null == regression) {
			mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
			throw new MRMLinearModelNotFoundException(
					MRMIndexes.getValue("EXC_MSG"));
		} else {
			double[] beta = regression.estimateRegressionParameters();
			for (double value : beta) {
				coefficients.add(value);
			}
		}
		return coefficients;
	}

	/**
	 * This method returns the coefficients of particular columns from the
	 * model.
	 * 
	 * @param regression
	 * @param columnNames
	 * @return coefficients
	 * @throws MRMLinearModelNotFoundException
	 */
	public Map<String, Double> getColumnCoefficients(
			OLSMultipleLinearRegression regression, String[] columnNames)
			throws MRMLinearModelNotFoundException {
		Map<String, Double> coefficients = new HashMap<>();
		if (null == regression) {
			mrmDevLogger.error(MRMIndexes.getValue("EXC_MSG"));
			throw new MRMLinearModelNotFoundException(
					MRMIndexes.getValue("EXC_MSG"));
		} else {
			double[] beta = regression.estimateRegressionParameters();
			if (null != beta && null != columnNames) {
				for (int betaValue = 0; betaValue < beta.length; betaValue++) {
					coefficients.put(columnNames[betaValue], beta[betaValue]);
				}
			}
		}
		return coefficients;
	}

	/**
	 * This will return the regression model based on the matrix.
	 * 
	 * @param dependentMatrix
	 * @param independentMatrix
	 * @return regression
	 */
	public OLSMultipleLinearRegression getRegressionModel(
			double[] dependentMatrix, double[][] independentMatrix) {
		OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
		regression.newSampleData(dependentMatrix, independentMatrix);
		return regression;
	}

	/**
	 * This method returns the exclude column lists based on the max value.
	 * 
	 * @param excludeColumnsList
	 * @param maxVifValues
	 * @param inclusiveVar
	 * @return excludeColumnsList
	 * @throws MRMVifNotFoundException
	 */
	private List<String> getExcludeList(List<String> excludeColumnsList,
			Map<String, Double> maxVifValues, String inclusiveVar)
			throws MRMVifNotFoundException {
		if (null == maxVifValues || maxVifValues.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VIF_VALUE"));
			throw new MRMVifNotFoundException(
					MRMIndexes.getValue("EMPTY_VIF_VALUE"));
		} else {
			Collection<Double> maxValues = maxVifValues.values();
			for (String columnValue : maxVifValues.keySet()) {
				String trimmedColumn = columnValue.trim();
				if (null != trimmedColumn
						&& !trimmedColumn.equals(inclusiveVar)) {
					if (Collections.max(maxValues) == maxVifValues
							.get(trimmedColumn)) {
						excludeColumnsList.add(trimmedColumn);
					}
				}
			}
		}
		return excludeColumnsList;
	}

	/**
	 * This method checks if the inclusive variables value is greater than 10 ,
	 * then there is a chance of all other variables are less than 10. In that
	 * case just skip the process.
	 * 
	 * @param maxVifValues
	 * @param cutOff
	 * @throws MRMVifNotFoundException
	 */
	private boolean checkVifValuesRange(Map<String, Double> maxVifValues,
			int cutOff) throws MRMVifNotFoundException {
		double value;
		if (null == maxVifValues || maxVifValues.isEmpty()) {
			mrmDevLogger.error(MRMIndexes.getValue("EMPTY_VIF_VALUE"));
			throw new MRMVifNotFoundException(
					MRMIndexes.getValue("EMPTY_VIF_VALUE"));
		} else {
			Collection<Double> maxValues = maxVifValues.values();
			value = Collections.max(maxValues);
		}
		return value < cutOff ? true : false;
	}
}
