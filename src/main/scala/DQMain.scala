package com.dq.pipeline

import com.dq.pipeline.Helpers.{DatabaseHelper, SparkHelper}
import com.dq.pipeline.Nodes.ConsolidatedDQReportNode
import com.dq.pipeline.utils.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/**
 * Main entry point for Data Quality analysis
 */
object DQMain {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  def main(args: Array[String]): Unit = {
    // Load .env file FIRST before any config is accessed
    ConfigLoader.loadEnv()
    
    // Workaround for Java 17+ getSubject() issue with Hadoop
    System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name"))
    
    logger.info("Starting VaR Data Quality Application...")
    
    // Test database connection
    if (!DatabaseHelper.testConnection()) {
      logger.error("Database connection test failed. Exiting...")
      System.exit(1)
    }
    
    // Print database configuration
    logger.info("Database Configuration:")
    DatabaseHelper.getDatabaseConfig.foreach { case (key, value) =>
      if (key != "password") {
        logger.info(s"  $key: $value")
      }
    }
    
    // Create Spark session
    val spark: SparkSession = SparkHelper.createSparkSession()
    
    try {
      // Generate consolidated data quality report
      logger.info("=== Generating Consolidated Data Quality Report ===")
      val consolidatedDQNode = new ConsolidatedDQReportNode()
      val dqReport = consolidatedDQNode.generateReport()
      
      logger.info("=== Data Quality Report ===")
      dqReport.show(100, truncate = false)
      
      // Persist report to database
      logger.info("=== Persisting Data Quality Report to Database ===")
      DatabaseHelper.writeDQReport(dqReport, mode = "append")
      logger.info("Data Quality Report successfully persisted to database")
      
    } catch {
      case e: Exception =>
        logger.error("Error during data quality analysis", e)
        System.exit(1)
    } finally {
      SparkHelper.stopSparkSession(spark)
    }
  }
}
