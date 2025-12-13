package com.dq.pipeline.Helpers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, count, explode, lit, struct, sum, when}
import org.slf4j.{Logger, LoggerFactory}
/**
 * DQArithmeticAgent provides utilities for performing arithmetic operations
 * on DataFrames for data quality analysis.
 */
object DQArithmeticAgent {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  //Function that counts not null elements in each column of a DataFrame
  def countNotNullSeqStringMapping(df: DataFrame, cols: Seq[String]): DataFrame = {
    logger.debug(s"Counting non-null values for columns: ${cols.mkString(",")}")

    val sumNotNullValues = cols.map { c =>
      // Count only true matches; otherwise return null so the row is not counted.
      val nonNull = when(
        col(c).isNotNull &&
        col(c).cast("string") =!= "NULL" &&
        col(c).cast("string") =!= "Null" &&
        col(c).cast("string") =!= "null",
        lit(1)
      ).otherwise(lit(null))

      sum(nonNull).alias(c)
    }

    df.select(sumNotNullValues: _*)
  }

  //Function that counts the total number of rows in a column including null values
  def totalCountSeqStringMapping(df: DataFrame, cols: Seq[String]): DataFrame = {
    logger.debug(s"Counting total rows for columns: ${cols.mkString(",")}")

    val totalValues = cols.map(c => count(lit(1)).alias(c))

    df.select(totalValues: _*)
  }

  /* 
  Function that transposses the count. Below is the logic
  Given a table that has column names and the counts as below
  id|name|location
  2 | 54 |   10
  ***************************************************
  The transpose will be
  Logical_Attribute|val
  id               |2
  name             |54
  location         |10
   */
  def dfTranspose(DF: DataFrame, by: Seq[String]): DataFrame = {
    logger.debug(s"Transposing DataFrame by keys: ${by.mkString(",")}")

    val (cols, types) = DF.dtypes.filter{ case (c, _) => !by.contains(c)}.unzip
    require(types.distinct.size ==1, s"${types.distinct.toString}.length != 1")

    val keyValue = explode(array(
        cols.map(c => struct(lit(c).alias("LogicalAttribute"), col(c).alias("val"))): _*
    ))

    val byExprs = by.map(col(_))

    DF.select(byExprs :+ keyValue.alias("keyValues"): _*)
    .withColumn("LogicalAttribute", col("keyValues.LogicalAttribute"))
    .withColumn("val", col("keyValues.val"))
    .drop("keyValues")
  }
}
