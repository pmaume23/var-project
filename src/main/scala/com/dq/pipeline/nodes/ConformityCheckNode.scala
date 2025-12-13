package com.dq.pipeline.Nodes

import com.dq.pipeline.Helpers.LoadSP500Data
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}

/**
 * ConformityCheckNode performs conformity checks on the S&P 500 dataset.
 * Validates data conforms to expected formats and patterns.
 */
class ConformityCheckNode(sp500DataDF: DataFrame = LoadSP500Data.sp500DataDF) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Load S&P 500 data
  private val spark = sp500DataDF.sparkSession

    /**
     * Generate conformity check report
     * @return DataFrame with conformity check results
     */
    def conformityReportDF(): DataFrame = {
      val totalRows = sp500DataDF.count()
      logger.info(s"Building conformity report for $totalRows rows")
        
        // Rule 1: Date format - Should conform to YYYY-MM-DD pattern
        val datePattern = "^\\d{4}-\\d{2}-\\d{2}$"
        val validDateFormat = sp500DataDF.filter(col("Date").rlike(datePattern)).count()
        val invalidDateFormat = totalRows - validDateFormat
        logger.debug(s"Date format valid=$validDateFormat invalid=$invalidDateFormat")
        
        // Rule 2: Ticker length - Should be 1-5 characters (standard stock ticker format)
        val validTickerLength = sp500DataDF.filter(length(col("Ticker")).between(1, 5)).count()
        val invalidTickerLength = totalRows - validTickerLength
        logger.debug(s"Ticker length valid=$validTickerLength invalid=$invalidTickerLength")
        
        // Rule 3: Open price - Should not contain NaN
        val validOpenPrice = sp500DataDF.filter(!isnan(col("Open"))).count()
        val invalidOpenPrice = totalRows - validOpenPrice
        logger.debug(s"Open price valid=$validOpenPrice invalid=$invalidOpenPrice")
        
        // Rule 4: Close price - Should not contain NaN
        val validClosePrice = sp500DataDF.filter(!isnan(col("Close"))).count()
        val invalidClosePrice = totalRows - validClosePrice
        logger.debug(s"Close price valid=$validClosePrice invalid=$invalidClosePrice")
        
        // Rule 5: Volume - Should not contain NaN
        val validVolume = sp500DataDF.filter(!isnan(col("Volume"))).count()
        val invalidVolume = totalRows - validVolume
        logger.debug(s"Volume valid=$validVolume invalid=$invalidVolume")
        
        // Create conformity report
        val conformityData = Seq(
          ("Date format (YYYY-MM-DD)", validDateFormat, invalidDateFormat, totalRows, "Conformity"),
          ("Ticker length (1-5 chars)", validTickerLength, invalidTickerLength, totalRows, "Conformity"),
          ("Open price (no NaN)", validOpenPrice, invalidOpenPrice, totalRows, "Conformity"),
          ("Close price (no NaN)", validClosePrice, invalidClosePrice, totalRows, "Conformity"),
          ("Volume (no NaN)", validVolume, invalidVolume, totalRows, "Conformity")
        )
        
        val reportDF = spark.createDataFrame(conformityData)
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
