package com.dq.pipeline.Nodes

import com.dq.pipeline.Helpers.LoadSP500Data
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, current_date, lit}
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}

/**
 * UniquenessCheckNode performs uniqueness checks on the S&P 500 dataset.
 * Based on analysis, Ticker + Date forms a natural composite primary key.
 */
class UniquenessCheckNode(sp500DataDF: DataFrame = LoadSP500Data.sp500DataDF) {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    private val spark = sp500DataDF.sparkSession

    /**
     * Check uniqueness of Ticker + Date composite key
     * @return DataFrame with uniqueness check results
     */
    def uniquenessReportDF(): DataFrame = {
        val totalRows = sp500DataDF.count()
        logger.info(s"Building uniqueness report for $totalRows rows")
        
        // Find duplicate combinations
        val duplicates = sp500DataDF
          .groupBy("Ticker", "Date")
          .agg(count("*").alias("count"))
          .filter(col("count") > 1)
        
        val duplicateCount = duplicates.count()
        val uniqueCount = totalRows - duplicateCount
        logger.debug(s"Duplicate composite keys: $duplicateCount")
         
        // Create uniqueness report
        val reportDF = spark.createDataFrame(Seq(
          ("Ticker + Date", uniqueCount, duplicateCount, totalRows, "Uniqueness")
        )).toDF("LogicalAttribute", "Pass", "Fail", "Total", "CheckType")
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
