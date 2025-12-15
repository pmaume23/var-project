package com.`var`.pipeline.nodes

import com.dq.pipeline.Helpers.LoadSP500Data
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, collect_list, expr, lag, lit, log, sqrt}

class VarSvarNode(sp500DataDF: DataFrame = LoadSP500Data.sp500DataDF) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  logger.info("VarSvarNode initialized.")
  /* 
  Filter the data frame to below columns:
    * Ticker
    * date
    * close
    * quarter
    * year
  Then return the filtered DataFrame.
   */
  val filteredDF: DataFrame = {
    logger.info("Filtering DataFrame to selected columns.")
    sp500DataDF.select("Ticker", "Date", "Close", "quarter", "year")
  }

  /* 
  Ticker and date window partitioning
   */
  logger.info("Setting up window partitioning by Ticker and Date.")
  val windowPartitioning = Window
    .partitionBy("Ticker")
    .orderBy("Date")

  /* 
  Calculate returns
   */
  logger.info("Calculating returns DataFrame.")
  val returnsDF: DataFrame = {
    logger.info("Calculating returns.")
    filteredDF
      .withColumn("PrevClose", lag("Close", 1).over(windowPartitioning))
      .withColumn("LogReturn",
        log(col("Close") / col("PrevClose"))
      )
      .filter(col("PrevClose").isNotNull)
  }

  /* 
  Build portfolio returns (equal-weighted)
   */
  logger.info("Calculating portfolio returns DataFrame.")
  val portfolioReturnsDF: DataFrame = {
    returnsDF
      .groupBy("Date")
      .agg(avg("LogReturn").alias("PortfolioLogReturn"))
      .orderBy("Date")
  }

  // Date metadata for joins
  private val dateMetadata: DataFrame = filteredDF
    .select("Date", "quarter", "year")
    .distinct()

  /* 
  Time-series VaR: Rolling 252-day (1 year) window
  For each date, calculate VaR using previous 252 trading days
   */
  logger.info("Calculating Rolling VaR time series with 252-day window.")
  val rollingWindowSpec = Window
    .orderBy("Date")
    .rowsBetween(-252, -1)  // Look back 252 days (1 trading year)

  val rollingVarDF: DataFrame = {
    portfolioReturnsDF
      .join(dateMetadata, Seq("Date"), "left")
      .withColumn("Rolling252DayReturns", 
        collect_list("PortfolioLogReturn").over(rollingWindowSpec)
      )
      .withColumn("WindowSize", 
        expr("size(Rolling252DayReturns)")
      )
      .filter(col("WindowSize") >= 252)  // Only keep windows with full 252 days
      .withColumn("NegReturns",
        expr("transform(Rolling252DayReturns, x -> -x)")
      )
      .withColumn("SortedNegReturns",
        expr("array_sort(NegReturns)")
      )
      .withColumn("PercentileIndex",
        expr("cast(ceil(size(SortedNegReturns) * 0.99) as int)")
      )
      .withColumn("VaR1Day99Rolling",
        expr("element_at(SortedNegReturns, PercentileIndex)")
      )
      .withColumn("VaR10Day99Rolling",
        sqrt(lit(10)) * col("VaR1Day99Rolling")
      )
      .select("Date", "quarter", "year", "PortfolioLogReturn", "VaR1Day99Rolling", "VaR10Day99Rolling")
      .filter(col("year").isin(2008, 2020))
      .orderBy("Date")
  }

  /* 
  Rolling Stressed VaR: Same rolling window but only using stressed period data
  Stressed periods: 2008 (Financial Crisis), 2020 (COVID-19 Crash)
   */
  logger.info("Calculating Rolling Stressed VaR using crisis periods (2008, 2020).")
  val stressedReturnsDF: DataFrame = {
    portfolioReturnsDF
      .join(dateMetadata, Seq("Date"), "left")
      .filter(col("year").isin(2008, 2020))
  }

  val rollingStressedVarDF: DataFrame = {
    // Rolling window over stressed periods only
    val stressedWindowSpec = Window
      .orderBy("Date")
      .rowsBetween(-252, -1)
    
    stressedReturnsDF
      .withColumn("StressedRolling252DayReturns", 
        collect_list("PortfolioLogReturn").over(stressedWindowSpec)
      )
      .withColumn("StressedWindowSize", 
        expr("size(StressedRolling252DayReturns)")
      )
      .filter(col("StressedWindowSize") >= 20)  // Require at least 20 stressed days
      .withColumn("StressedNegReturns",
        expr("transform(StressedRolling252DayReturns, x -> -x)")
      )
      .withColumn("StressedSortedNegReturns",
        expr("array_sort(StressedNegReturns)")
      )
      .withColumn("StressedPercentileIndex",
        expr("cast(ceil(size(StressedSortedNegReturns) * 0.99) as int)")
      )
      .withColumn("SVaR1Day99Rolling",
        expr("element_at(StressedSortedNegReturns, StressedPercentileIndex)")
      )
      .withColumn("SVaR10Day99Rolling",
        sqrt(lit(10)) * col("SVaR1Day99Rolling")
      )
      .select("Date", "quarter", "year", "PortfolioLogReturn", "SVaR1Day99Rolling", "SVaR10Day99Rolling")
      .orderBy("Date")
  }

  // Combined view with both VaR and Stressed VaR columns
  val combinedVarSVarDF: DataFrame = {
    rollingVarDF
      .join(
        rollingStressedVarDF.select("Date", "SVaR1Day99Rolling", "SVaR10Day99Rolling"),
        Seq("Date"),
        "left"
      )
      .orderBy("Date")
  }
  
}
