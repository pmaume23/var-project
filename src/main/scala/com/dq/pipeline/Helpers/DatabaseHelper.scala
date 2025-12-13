package com.dq.pipeline.Helpers

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, DriverManager}
import java.util.Properties

/**
 * DatabaseHelper provides utilities for loading data from PostgreSQL database
 * using Spark JDBC connectors.
 */
object DatabaseHelper {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val config: Config = ConfigFactory.load()
  
  // Database configuration
  private val dbHost: String = config.getString("database.host")
  private val dbPort: Int = config.getInt("database.port")
  private val dbName: String = config.getString("database.name")
  private val dbUser: String = config.getString("database.user")
  private val dbPassword: String = config.getString("database.password")
  private val dbDriver: String = config.getString("database.driver")
  
  private val jdbcUrl: String = s"jdbc:postgresql://$dbHost:$dbPort/$dbName"
  
  /**
   * Get JDBC connection properties
   */
  private def getConnectionProperties: Properties = {
    val properties = new Properties()
    properties.setProperty("user", dbUser)
    properties.setProperty("password", dbPassword)
    properties.setProperty("driver", dbDriver)
    properties
  }
  
  /**
   * Load data from a PostgreSQL table using Spark JDBC
   * 
   * @param spark SparkSession instance
   * @param tableName Name of the table to load
   * @return DataFrame containing the table data
   */
  def loadTable(spark: SparkSession, tableName: String): DataFrame = {
    logger.info(s"Loading table: $tableName from database: $dbName")
    
    try {
      val df = spark.read
        .jdbc(jdbcUrl, tableName, getConnectionProperties)
      
      logger.info(s"Successfully loaded table: $tableName with ${df.count()} rows")
      df
    } catch {
      case e: Exception =>
        logger.error(s"Failed to load table: $tableName", e)
        throw e
    }
  }
  
  /**
   * Load data from a PostgreSQL table with custom query
   * 
   * @param spark SparkSession instance
   * @param query SQL query to execute
   * @return DataFrame containing the query results
   */
  def loadQuery(spark: SparkSession, query: String): DataFrame = {
    logger.info(s"Executing query: ${query.take(100)}...")
    
    try {
      val df = spark.read
        .jdbc(jdbcUrl, s"($query) AS subquery", getConnectionProperties)
      
      logger.info(s"Successfully executed query, returned ${df.count()} rows")
      df
    } catch {
      case e: Exception =>
        logger.error(s"Failed to execute query", e)
        throw e
    }
  }
  
  /**
   * Load data with partitioning for better performance
   * 
   * @param spark SparkSession instance
   * @param tableName Name of the table to load
   * @param partitionColumn Column to partition on
   * @param lowerBound Lower bound for partitioning
   * @param upperBound Upper bound for partitioning
   * @param numPartitions Number of partitions
   * @return DataFrame containing the table data
   */
  def loadTablePartitioned(
    spark: SparkSession,
    tableName: String,
    partitionColumn: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int
  ): DataFrame = {
    logger.info(s"Loading table: $tableName with partitioning on $partitionColumn")
    
    try {
      val df = spark.read
        .jdbc(
          url = jdbcUrl,
          table = tableName,
          columnName = partitionColumn,
          lowerBound = lowerBound,
          upperBound = upperBound,
          numPartitions = numPartitions,
          connectionProperties = getConnectionProperties
        )
      
      logger.info(s"Successfully loaded partitioned table: $tableName")
      df
    } catch {
      case e: Exception =>
        logger.error(s"Failed to load partitioned table: $tableName", e)
        throw e
    }
  }
  
  /**
   * Write DataFrame to PostgreSQL table
   * 
   * @param df DataFrame to write
   * @param tableName Target table name
   * @param mode Write mode: "overwrite", "append", "ignore", "error"
   */
  def writeTable(df: DataFrame, tableName: String, mode: String = "overwrite"): Unit = {
    logger.info(s"Writing data to table: $tableName with mode: $mode")
    
    try {
      df.write
        .mode(mode)
        .jdbc(jdbcUrl, tableName, getConnectionProperties)
      
      logger.info(s"Successfully wrote data to table: $tableName")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to write to table: $tableName", e)
        throw e
    }
  }
  
  /**
   * Test database connection
   * 
   * @return true if connection is successful, false otherwise
   */
  def testConnection(): Boolean = {
    logger.info("Testing database connection...")
    
    var connection: Connection = null
    try {
      Class.forName(dbDriver)
      connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
      logger.info("Database connection successful!")
      true
    } catch {
      case e: Exception =>
        logger.error("Database connection failed", e)
        false
    } finally {
      if (connection != null && !connection.isClosed) {
        connection.close()
      }
    }
  }
  
  /**
   * Get database configuration details (for logging/debugging)
   */
  def getDatabaseConfig: Map[String, String] = {
    Map(
      "host" -> dbHost,
      "port" -> dbPort.toString,
      "database" -> dbName,
      "user" -> dbUser,
      "jdbcUrl" -> jdbcUrl
    )
  }
  
  /**
   * Load the S&P 500 ratios and price data table
   */
  def loadSP500Data(spark: SparkSession): DataFrame = {
    loadTable(spark, "sp500_ratios_price")
  }
  
  /**
   * Write data quality report to database table
   * 
   * @param dqReport DataFrame containing the DQ report with columns:
   *                  LogicalAttribute, Pass, Fail, Total, PercentagePass, PercentageFail, CheckType, DateChecked
   * @param mode Write mode: "overwrite", "append", "ignore", "error"
   */
  def writeDQReport(dqReport: DataFrame, mode: String = "append"): Unit = {
    logger.info(s"Writing DQ report to dq_report table with mode: $mode")
    
    try {
      // Map DataFrame columns to table columns (handle naming conventions)
      val reportDF = dqReport
        .withColumnRenamed("LogicalAttribute", "logical_attribute")
        .withColumnRenamed("Pass", "pass_count")
        .withColumnRenamed("Fail", "fail_count")
        .withColumnRenamed("Total", "total_count")
        .withColumnRenamed("PercentagePass", "percentage_pass")
        .withColumnRenamed("PercentageFail", "percentage_fail")
        .withColumnRenamed("CheckType", "check_type")
        .withColumnRenamed("DateChecked", "date_checked")
      
      // Cast percentage columns to DECIMAL and write to database
      reportDF
        .select(
          col("logical_attribute"), 
          col("pass_count"), 
          col("fail_count"), 
          col("total_count"),
          col("percentage_pass").cast("DECIMAL(10,4)").alias("percentage_pass"),
          col("percentage_fail").cast("DECIMAL(10,4)").alias("percentage_fail"),
          col("check_type"), 
          col("date_checked")
        )
        .write
        .mode(mode)
        .jdbc(jdbcUrl, "dq_report", getConnectionProperties)
      
      val rowCount = reportDF.count()
      logger.info(s"Successfully wrote $rowCount rows to dq_report table")
    } catch {
      case e: Exception =>
        logger.error("Failed to write DQ report to database", e)
        throw e
    }
  }
}
