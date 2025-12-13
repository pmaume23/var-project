package com.dq.pipeline.Helpers

import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
 * SparkHelper provides utilities for creating and configuring SparkSession
 */
object SparkHelper {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val config: Config = ConfigFactory.load()
  
  /**
   * Create a SparkSession with configurations from application.conf
   * 
   * @return Configured SparkSession instance
   */
  def createSparkSession(): SparkSession = {
    logger.info("Creating SparkSession...")
    
    val appName = config.getString("spark.appName")
    val master = config.getString("spark.master")
    
    // Java 17+ compatibility flags
    val javaOptions = "--add-opens=java.base/java.lang=ALL-UNNAMED " +
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
      "--add-opens=java.base/java.io=ALL-UNNAMED " +
      "--add-opens=java.base/java.net=ALL-UNNAMED " +
      "--add-opens=java.base/java.nio=ALL-UNNAMED " +
      "--add-opens=java.base/java.util=ALL-UNNAMED " +
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    
    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config("spark.sql.shuffle.partitions", config.getString("spark.sql.shuffle.partitions"))
      .config("spark.sql.adaptive.enabled", config.getString("spark.sql.adaptive.enabled"))
      .config("spark.executor.memory", config.getString("spark.executor.memory"))
      .config("spark.driver.memory", config.getString("spark.driver.memory"))
      .config("spark.driver.extraJavaOptions", javaOptions)
      .config("spark.executor.extraJavaOptions", javaOptions)
      .getOrCreate()
    
    logger.info(s"SparkSession created successfully: $appName")
    logger.info(s"Spark UI available at: ${spark.sparkContext.uiWebUrl.getOrElse("N/A")}")
    
    spark
  }
  
  /**
   * Stop SparkSession gracefully
   * 
   * @param spark SparkSession to stop
   */
  def stopSparkSession(spark: SparkSession): Unit = {
    logger.info("Stopping SparkSession...")
    spark.stop()
    logger.info("SparkSession stopped")
  }
  
  /**
   * Create a SparkSession with custom configurations
   * 
   * @param appName Application name
   * @param master Spark master URL
   * @param configs Additional configurations as key-value pairs
   * @return Configured SparkSession instance
   */
  def createCustomSparkSession(
    appName: String,
    master: String = "local[*]",
    configs: Map[String, String] = Map.empty
  ): SparkSession = {
    logger.info(s"Creating custom SparkSession: $appName")
    
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)
    
    // Apply custom configurations
    configs.foreach { case (key, value) =>
      builder.config(key, value)
    }
    
    val spark = builder.getOrCreate()
    logger.info(s"Custom SparkSession created: $appName")
    
    spark
  }
}
