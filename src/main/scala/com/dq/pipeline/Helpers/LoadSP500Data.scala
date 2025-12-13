package com.dq.pipeline.Helpers

import com.dq.pipeline.Helpers.{DatabaseHelper, SparkHelper}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

object LoadSP500Data {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //Create spark session
  logger.info("Creating SparkSession for S&P 500 data load")
  val spark = SparkHelper.createSparkSession()

  //Load S&P 500 data
  logger.info("Loading S&P 500 data from database")
  val sp500DataDF: DataFrame = DatabaseHelper.loadSP500Data(spark)
}
