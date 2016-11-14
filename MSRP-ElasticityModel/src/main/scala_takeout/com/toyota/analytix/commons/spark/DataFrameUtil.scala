package com.toyota.analytix.commons.spark

import org.apache.spark.sql.DataFrame
import scala.util.control.Breaks._
import com.toyota.analytix.common.exceptions.AnalytixRuntimeException
import org.apache.spark.sql.Row
import org.apache.log4j.Logger

object DataFrameUtil {
  def logger = Logger.getLogger(getClass().getName())
  /**
   * Method to return row value String by column name from single row data frames.
   */
  def getStringValueOfColumn(d: DataFrame, columnName: String): String = {
    getValueOfColumn[String](d, columnName)
  }

  /**
   * Method to return row value by column name from single row data frames.
   */
  def getValueOfColumn[T](d: DataFrame, columnName: String): T = {
    val rows = d.collectAsList()
    if (rows.size < 2) {
      throw new AnalytixRuntimeException(" Dataframe should have a header and value row");
    }
    val cols = d.columns

    val firstRow = d.collectAsList().get(1)
    getValueOfColumn(cols, firstRow, columnName);
  }

  /**
   * Method to get value of a column when column names are given
   */
  def getValueOfColumn[T](columnNames: Array[String], row: Row, columnName: String): T = {
    val cols = columnNames

    val firstRow = row

    var j: Int = -1;
    breakable {
      for (i <- cols.indices) {
        j = i //There got to be a better way to avoid this line
        if (cols(i).equalsIgnoreCase(columnName)) break;
      }
    }

    if (j < 0) {
      throw new AnalytixRuntimeException(columnName
        + " column not found in DateFrame with Columns:" + cols);
    }

    firstRow.getAs[T](j)
  }

}