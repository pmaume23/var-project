package com.dq.pipeline.Nodes

import com.dq.pipeline.testutil.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class ValidityCheckNodeSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("validity report tallies rule passes and fails") {
    logger.info("Testing validity report generation")
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
      Row("GOOD", "2021-01-01", 1.0, 1.2, 0.0, 1, 2021),
      Row("bad", "2021-01-02", 1.0, 0.1, -5.0, 5, 1999),
      Row("GOOD", "2021-01-03", -2.0, 1.0, 10.0, 2, 2021)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val report = new ValidityCheckNode(df).validityReportDF()

    def passFail(attr: String) = {
      val r = report.filter($"LogicalAttribute" === attr).head()
      (r.getAs[Long]("Pass"), r.getAs[Long]("Fail"))
    }

    assert(passFail("Ticker format (^[A-Z]+$)") == (2L, 1L))
    assert(passFail("quarter range [1-4]") == (2L, 1L))
    assert(passFail("year range [2000-2025]") == (2L, 1L))
    assert(passFail("Open price > 0") == (2L, 1L))
    assert(passFail("Close price > 0") == (3L, 0L))
    assert(passFail("Volume >= 0") == (2L, 1L))
    assert(passFail("Price change < 50%") == (1L, 2L))
    logger.info("Validity report test passed")
  }
}
