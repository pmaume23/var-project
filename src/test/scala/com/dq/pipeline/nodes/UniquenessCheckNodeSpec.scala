package com.dq.pipeline.Nodes

import com.dq.pipeline.testutil.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class UniquenessCheckNodeSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("uniqueness report detects duplicate Ticker + Date pairs") {
    logger.info("Testing uniqueness report generation")
    import spark.implicits._
    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", StringType, nullable = false),
      StructField("Close", DoubleType, nullable = false)
    ))

    val rows = Seq(
      Row("ABC", "2020-01-01", 10.0),
      Row("ABC", "2020-01-02", 11.0),
      Row("ABC", "2020-01-01", 10.0) // duplicate key
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val report = new UniquenessCheckNode(df).uniquenessReportDF()
    val row = report.head()

    assert(row.getAs[Long]("Pass") == 2L)
    assert(row.getAs[Long]("Fail") == 1L)
    logger.info("Uniqueness report test passed")
  }
}
