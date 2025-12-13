package com.dq.pipeline.Nodes

import com.dq.pipeline.Helpers.LoadSP500Data
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}

/**
 * ValidityCheckNode performs validity checks on the S&P 500 dataset.
 * Validates data against business rules and expected patterns.
 */
class ValidityCheckNode(sp500DataDF: DataFrame = LoadSP500Data.sp500DataDF) {

  // Load S&P 500 data
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val spark = sp500DataDF.sparkSession

    /**
     * Generate validity check report
     * @return DataFrame with validity check results
     */
    def validityReportDF(): DataFrame = {
        val totalRows = sp500DataDF.count()
        logger.info(s"Building validity report for $totalRows rows")
        
        // Rule 1: Ticker format - Should match pattern ^[A-Z]+$ (uppercase letters only)
        val tickerPattern = "^[A-Z]+$"
        val validTickers = sp500DataDF.filter(col("Ticker").rlike(tickerPattern)).count()
        val invalidTickers = totalRows - validTickers
        logger.debug(s"Ticker format valid=$validTickers invalid=$invalidTickers")
        
        // Rule 2: quarter range - Must be in [1, 2, 3, 4]
        val validQuarters = sp500DataDF.filter(col("quarter").isin(1, 2, 3, 4)).count()
        val invalidQuarters = totalRows - validQuarters
        logger.debug(s"Quarter range valid=$validQuarters invalid=$invalidQuarters")
        
        // Rule 3: year range - Must be in [2000, 2025]
        val validYears = sp500DataDF.filter(col("year").between(2000, 2025)).count()
        val invalidYears = totalRows - validYears
        logger.debug(s"Year range valid=$validYears invalid=$invalidYears")
        
        // Rule 4: Positive Open prices
        val validOpenPrices = sp500DataDF.filter(col("Open") > 0).count()
        val invalidOpenPrices = totalRows - validOpenPrices
        logger.debug(s"Open price valid=$validOpenPrices invalid=$invalidOpenPrices")
        
        // Rule 5: Positive Close prices
        val validClosePrices = sp500DataDF.filter(col("Close") > 0).count()
        val invalidClosePrices = totalRows - validClosePrices
        logger.debug(s"Close price valid=$validClosePrices invalid=$invalidClosePrices")
        
        // Rule 6: Non-negative Volume
        val validVolumes = sp500DataDF.filter(col("Volume") >= 0).count()
        val invalidVolumes = totalRows - validVolumes
        logger.debug(s"Volume valid=$validVolumes invalid=$invalidVolumes")
        
        // Rule 7: Price change threshold - |Open - Close| / Open < 50%
        val validPriceChanges = sp500DataDF.filter(
          col("Open") > 0 && (abs(col("Open") - col("Close")) / col("Open")) < 0.50
        ).count()
        val invalidPriceChanges = totalRows - validPriceChanges
        logger.debug(s"Price change valid=$validPriceChanges invalid=$invalidPriceChanges")
        
        // Create validity report
        val validityData = Seq(
          ("Ticker format (^[A-Z]+$)", validTickers, invalidTickers, totalRows, "Validity"),
          ("quarter range [1-4]", validQuarters, invalidQuarters, totalRows, "Validity"),
          ("year range [2000-2025]", validYears, invalidYears, totalRows, "Validity"),
          ("Open price > 0", validOpenPrices, invalidOpenPrices, totalRows, "Validity"),
          ("Close price > 0", validClosePrices, invalidClosePrices, totalRows, "Validity"),
          ("Volume >= 0", validVolumes, invalidVolumes, totalRows, "Validity"),
          ("Price change < 50%", validPriceChanges, invalidPriceChanges, totalRows, "Validity")
        )
        
        val reportDF = spark.createDataFrame(validityData)
          .toDF("LogicalAttribute", "Pass", "Fail", "Total", "CheckType")
          .withColumn("PercentagePass", 
            ((col("Pass").cast("double") / col("Total")) * 100)
              .cast("decimal(9,4)")
              .cast(StringType))
          .withColumn("PercentageFail", 
            ((col("Fail").cast("double") / col("Total")) * 100)
              .cast("decimal(9,4)")
              .cast(StringType))
          .withColumn("DateChecked", current_date())
          .select("LogicalAttribute", "Pass", "Fail", "Total", 
                  "PercentagePass", "PercentageFail", "CheckType", "DateChecked")
        
        reportDF
    }
}
