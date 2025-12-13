package com.dq.pipeline.Nodes

import com.dq.pipeline.testutil.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class ConsolidatedDQReportNodeSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("consolidated report combines all DQ dimensions") {
    logger.info("Testing consolidated DQ report generation")
    import spark.implicits._
    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", StringType, nullable = false),
      StructField("Open", DoubleType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("Volume", DoubleType, nullable = false),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    val rows = Seq(
      Row("AAA", "2020-01-01", 10.0, 11.0, 100.0, 1, 2020),
      Row("BBB", "2020-01-02", 9.0, 8.5, 200.0, 2, 2021)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val report = new ConsolidatedDQReportNode(df).generateReport()

    // 7 completeness + 1 uniqueness + 7 validity + 5 conformity = 20 rows expected
    val reportCount = report.count()
    logger.info(s"Consolidated report generated with $reportCount rows")
    assert(reportCount == 20L)
    logger.info("Consolidated DQ report test passed")
  }
}
