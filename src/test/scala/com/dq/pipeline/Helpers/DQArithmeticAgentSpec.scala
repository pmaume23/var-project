package com.dq.pipeline.Helpers

import com.dq.pipeline.testutil.SparkTestSession
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class DQArithmeticAgentSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("countNotNullSeqStringMapping and totalCountSeqStringMapping handle nulls") {
    logger.info("Testing countNotNull and totalCount functions")
    import spark.implicits._
    val df = Seq(
      ("A", 1: java.lang.Integer),
      (null.asInstanceOf[String], null.asInstanceOf[Integer])
    ).toDF("col1", "col2")

    val notNull = DQArithmeticAgent.countNotNullSeqStringMapping(df, Seq("col1", "col2")).collect().head
    val totals = DQArithmeticAgent.totalCountSeqStringMapping(df, Seq("col1", "col2")).collect().head

    assert(notNull.getAs[Long]("col1") == 1L)
    assert(notNull.getAs[Long]("col2") == 1L)
    assert(totals.getAs[Long]("col1") == 2L)
    assert(totals.getAs[Long]("col2") == 2L)
    logger.info("countNotNull and totalCount tests passed")
  }

  test("dfTranspose pivots wide counts into LogicalAttribute and val") {
    logger.info("Testing dataframe transpose function")
    import spark.implicits._
    val df = Seq((2L, 54L, 10L)).toDF("id", "name", "location")
    val transposed = DQArithmeticAgent.dfTranspose(df, Seq.empty)

    val result = transposed.select("LogicalAttribute", "val").collect().map(r => r.getString(0) -> r.getLong(1)).toSet
    assert(result == Set("id" -> 2L, "name" -> 54L, "location" -> 10L))
    logger.info("Dataframe transpose test passed")
  }
}
