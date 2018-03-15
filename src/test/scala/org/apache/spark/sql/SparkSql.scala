package org.apache.spark.sql

object SparkSql {

  def showString(df: DataFrame, numRows: Int = 1): String = {
    df.showString(numRows)
  }

}
