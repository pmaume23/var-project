package com.dq.pipeline.testutil

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Provides a shared local SparkSession for ScalaTest suites.
 */
trait SparkTestSession extends BeforeAndAfterAll { this: Suite =>
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  protected lazy val spark: SparkSession = {
    logger.info("Creating test SparkSession")
    System.setProperty("HADOOP_USER_NAME", System.getProperty("user.name"))
    System.setProperty("java.security.manager", "allow")
    val session = SparkSession.builder()
    .appName("dq-tests")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
    logger.info("Test SparkSession created successfully")
    session
  }

  override protected def beforeAll(): Unit = {
    logger.info(s"Starting test suite: ${this.getClass.getSimpleName}")
    spark // force initialization before tests run
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    logger.info(s"Finishing test suite: ${this.getClass.getSimpleName}")
    if (!spark.sparkContext.isStopped) {
      logger.info("Stopping test SparkSession")
      spark.stop()
    }
    super.afterAll()
  }
}
