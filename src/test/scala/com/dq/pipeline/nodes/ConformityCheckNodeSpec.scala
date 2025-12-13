package com.dq.pipeline.Nodes

import com.dq.pipeline.testutil.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class ConformityCheckNodeSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("conformity report covers date format, ticker length, and NaN checks") {
    logger.info("Testing conformity report generation")
    import spark.implicits._
    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", StringType, nullable = false),
      StructField("Open", DoubleType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("Volume", DoubleType, nullable = false)
    ))

    val rows = Seq(
      Row("ABC", "2020-01-01", 10.0, 11.0, 100.0),
      Row("TOOLONG", "2020/01/02", Double.NaN, 9.0, 5.0),
      Row("XY", "2020-02-03", 5.0, Double.NaN, 7.0)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val report = new ConformityCheckNode(df).conformityReportDF()

    def passFail(attr: String) = {
      val r = report.filter($"LogicalAttribute" === attr).head()
      (r.getAs[Long]("Pass"), r.getAs[Long]("Fail"))
    }

    assert(passFail("Date format (YYYY-MM-DD)") == (2L, 1L))
    assert(passFail("Ticker length (1-5 chars)") == (2L, 1L))
    assert(passFail("Open price (no NaN)") == (2L, 1L))
    assert(passFail("Close price (no NaN)") == (2L, 1L))
    assert(passFail("Volume (no NaN)") == (3L, 0L))
    logger.info("Conformity report test passed")
  }
}
