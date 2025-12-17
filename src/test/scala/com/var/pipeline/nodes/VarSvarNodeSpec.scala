package com.`var`.pipeline.nodes

import com.dq.pipeline.testutil.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Date

class VarSvarNodeSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("VarSvarNode calculates portfolio returns from multi-ticker data") {
    logger.info("Testing portfolio returns calculation")
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", DateType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    val rows = Seq(
      Row("AAPL", Date.valueOf("2020-01-02"), 100.0, 1, 2020),
      Row("AAPL", Date.valueOf("2020-01-03"), 105.0, 1, 2020),
      Row("AAPL", Date.valueOf("2020-01-04"), 110.0, 1, 2020),
      Row("MSFT", Date.valueOf("2020-01-02"), 200.0, 1, 2020),
      Row("MSFT", Date.valueOf("2020-01-03"), 210.0, 1, 2020),
      Row("MSFT", Date.valueOf("2020-01-04"), 220.0, 1, 2020)
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val varSvarNode = new VarSvarNode(testDF)

    // Check portfolio returns DataFrame
    val portfolioReturns = varSvarNode.portfolioReturnsDF.collect()
    
    // Should have 2 dates with returns (2020-01-03 and 2020-01-04)
    assert(portfolioReturns.length == 2)
    
    // Portfolio return should be average of log returns
    val firstReturn = portfolioReturns.find(_.getAs[Date]("Date") == Date.valueOf("2020-01-03"))
    assert(firstReturn.isDefined)
    assert(firstReturn.get.getAs[Double]("PortfolioLogReturn") != 0.0)
    
    logger.info("Portfolio returns calculation test passed")
  }

  test("VarSvarNode calculates VaR with sufficient data") {
    logger.info("Testing VaR calculation with adequate rolling window")
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", DateType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    // Generate 300 days of data to ensure rolling window has enough history
    val startDate = Date.valueOf("2008-01-01")
    val rows = (0 until 300).flatMap { dayOffset =>
      val date = new Date(startDate.getTime + dayOffset * 24L * 3600L * 1000L)
      val quarter = (date.getMonth / 3) + 1
      val year = 2008
      
      // Two tickers with some volatility
      val aaplClose = 100.0 + scala.math.sin(dayOffset * 0.1) * 5.0
      val msftClose = 200.0 + scala.math.cos(dayOffset * 0.1) * 8.0
      
      Seq(
        Row("AAPL", date, aaplClose, quarter, year),
        Row("MSFT", date, msftClose, quarter, year)
      )
    }

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val varSvarNode = new VarSvarNode(testDF)

    // Check that rolling VaR DataFrame is calculated
    val rollingVar = varSvarNode.rollingVarDF.collect()
    
    // Should have rows (after filtering for 252-day window and year 2008)
    assert(rollingVar.nonEmpty, "VaR DataFrame should not be empty with sufficient data")
    
    // Check that VaR1Day99Rolling and VaR10Day99Rolling columns exist
    val firstRow = rollingVar.head
    assert(firstRow.schema.fieldNames.contains("VaR1Day99Rolling"))
    assert(firstRow.schema.fieldNames.contains("VaR10Day99Rolling"))
    
    // VaR10Day should be sqrt(10) * VaR1Day (approximately 3.162x)
    val var1Day = firstRow.getAs[Double]("VaR1Day99Rolling")
    val var10Day = firstRow.getAs[Double]("VaR10Day99Rolling")
    assert(var1Day > 0.0, "VaR1Day should be positive")
    assert(math.abs(var10Day / var1Day - math.sqrt(10)) < 0.01, "VaR10Day should be sqrt(10) * VaR1Day")
    
    logger.info("VaR calculation test passed")
  }

  test("VarSvarNode calculates Stressed VaR for crisis periods") {
    logger.info("Testing Stressed VaR calculation")
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", DateType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    // Generate 100 days of stressed period data (2008)
    val startDate = Date.valueOf("2008-01-01")
    val rows = (0 until 100).flatMap { dayOffset =>
      val date = new Date(startDate.getTime + dayOffset * 24L * 3600L * 1000L)
      val quarter = (date.getMonth / 3) + 1
      val year = 2008
      
      // Simulate higher volatility for stressed period
      val aaplClose = 100.0 + scala.math.sin(dayOffset * 0.2) * 15.0
      val msftClose = 200.0 + scala.math.cos(dayOffset * 0.2) * 25.0
      
      Seq(
        Row("AAPL", date, aaplClose, quarter, year),
        Row("MSFT", date, msftClose, quarter, year)
      )
    }

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val varSvarNode = new VarSvarNode(testDF)

    // Check that rolling stressed VaR DataFrame is calculated
    val stressedVar = varSvarNode.rollingStressedVarDF.collect()
    
    // Should have rows (requires at least 20 stressed days)
    assert(stressedVar.nonEmpty, "Stressed VaR DataFrame should not be empty")
    
    // Check that SVaR columns exist
    val firstRow = stressedVar.head
    assert(firstRow.schema.fieldNames.contains("SVaR1Day99Rolling"))
    assert(firstRow.schema.fieldNames.contains("SVaR10Day99Rolling"))
    
    // SVaR10Day should be sqrt(10) * SVaR1Day
    val svar1Day = firstRow.getAs[Double]("SVaR1Day99Rolling")
    val svar10Day = firstRow.getAs[Double]("SVaR10Day99Rolling")
    assert(svar1Day > 0.0, "SVaR1Day should be positive")
    assert(math.abs(svar10Day / svar1Day - math.sqrt(10)) < 0.01, "SVaR10Day should be sqrt(10) * SVaR1Day")
    
    logger.info("Stressed VaR calculation test passed")
  }

  test("VarSvarNode produces combined VaR and SVaR DataFrame") {
    logger.info("Testing combined VaR and SVaR DataFrame")
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", DateType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    // Generate sufficient data for both VaR and SVaR (2008 data)
    val startDate = Date.valueOf("2008-01-01")
    val rows = (0 until 300).flatMap { dayOffset =>
      val date = new Date(startDate.getTime + dayOffset * 24L * 3600L * 1000L)
      val quarter = (date.getMonth / 3) + 1
      val year = 2008
      
      val aaplClose = 100.0 + scala.math.sin(dayOffset * 0.1) * 10.0
      val msftClose = 200.0 + scala.math.cos(dayOffset * 0.1) * 15.0
      
      Seq(
        Row("AAPL", date, aaplClose, quarter, year),
        Row("MSFT", date, msftClose, quarter, year)
      )
    }

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val varSvarNode = new VarSvarNode(testDF)

    // Check combined DataFrame
    val combined = varSvarNode.combinedVarSVarDF.collect()
    
    assert(combined.nonEmpty, "Combined DataFrame should not be empty")
    
    // Check that both VaR and SVaR columns exist
    val firstRow = combined.head
    val expectedColumns = Set("Date", "VaR1Day99Rolling", "VaR10Day99Rolling", "SVaR1Day99Rolling", "SVaR10Day99Rolling")
    val actualColumns = firstRow.schema.fieldNames.toSet
    
    assert(expectedColumns.subsetOf(actualColumns), s"Combined DataFrame should contain VaR and SVaR columns. Found: ${actualColumns.mkString(", ")}")
    
    logger.info("Combined VaR and SVaR DataFrame test passed")
  }

  test("VarSvarNode filtered DataFrame contains only required columns") {
    logger.info("Testing filtered DataFrame structure")
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", DateType, nullable = false),
      StructField("Open", DoubleType, nullable = true),
      StructField("Close", DoubleType, nullable = false),
      StructField("Volume", DoubleType, nullable = true),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    val rows = Seq(
      Row("AAPL", Date.valueOf("2020-01-02"), 98.0, 100.0, 1000000.0, 1, 2020),
      Row("MSFT", Date.valueOf("2020-01-02"), 198.0, 200.0, 2000000.0, 1, 2020)
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val varSvarNode = new VarSvarNode(testDF)

    // Check filtered DataFrame has only required columns
    val filtered = varSvarNode.filteredDF
    val columns = filtered.columns.toSet
    val expectedColumns = Set("Ticker", "Date", "Close", "quarter", "year")
    
    assert(columns == expectedColumns, s"Filtered DataFrame should only have required columns. Found: ${columns.mkString(", ")}")
    
    logger.info("Filtered DataFrame structure test passed")
  }

  test("VarSvarNode handles single ticker data") {
    logger.info("Testing VarSvarNode with single ticker")
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = false),
      StructField("Date", DateType, nullable = false),
      StructField("Close", DoubleType, nullable = false),
      StructField("quarter", IntegerType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    val rows = Seq(
      Row("AAPL", Date.valueOf("2020-01-02"), 100.0, 1, 2020),
      Row("AAPL", Date.valueOf("2020-01-03"), 105.0, 1, 2020),
      Row("AAPL", Date.valueOf("2020-01-04"), 110.0, 1, 2020)
    )

    val testDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val varSvarNode = new VarSvarNode(testDF)

    // Should still produce portfolio returns (even with single ticker)
    val portfolioReturns = varSvarNode.portfolioReturnsDF.collect()
    assert(portfolioReturns.length == 2, "Should have 2 return observations")
    
    logger.info("Single ticker test passed")
  }
}
