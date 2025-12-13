package com.dq.pipeline.Nodes

import com.dq.pipeline.Helpers.{DQArithmeticAgent, LoadSP500Data}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_date, lit, round}
import org.apache.spark.sql.types.StringType
import org.slf4j.{Logger, LoggerFactory}
/**
 * CompletenessCheckNode performs completeness checks on the S&P 500 dataset.
 */
class CompletenessCheckNode(sp500DataDF: DataFrame = LoadSP500Data.sp500DataDF) {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    //Get column names
    val DFColNames: Seq[String] = sp500DataDF.columns.toSeq
    logger.info(s"Initializing CompletenessCheckNode with ${DFColNames.size} columns")
    
    //Count not null rows
    val countNotNullDF = DQArithmeticAgent.countNotNullSeqStringMapping(sp500DataDF, DFColNames)
    .withColumn("notNullID", lit("1"))

    //Count total rows
    val countTotalDF = DQArithmeticAgent.totalCountSeqStringMapping(sp500DataDF, DFColNames)
    .withColumn("totalNullID", lit("2"))

    //Transpose not null counts DataFrame
    val notNullTransposedDF = DQArithmeticAgent.dfTranspose(countNotNullDF, Seq("notNullID"))
    .withColumnRenamed("val", "TotalNotNull")
    .withColumnRenamed("LogicalAttribute", "LogicalAttributePass")
    .drop("notNullID")

    //Transpose the total number of rows count
    val totalTransposedDF = DQArithmeticAgent.dfTranspose(countTotalDF, Seq("totalNullID"))
    .withColumnRenamed("val", "Total")
    .drop("totalNullID")

    //Create completeness check report
    def completenessReportDF(): DataFrame = {
        logger.info("Building completeness report DataFrame")

        val passFailReportDF: DataFrame = totalTransposedDF
        .join(notNullTransposedDF,
        totalTransposedDF("LogicalAttribute") === notNullTransposedDF("LogicalAttributePass"), "left_outer")
        .withColumn("Pass", col("TotalNotNull"))
        .withColumn("Fail", col("Total") - col("TotalNotNull"))
        .drop("LogicalAttributePass", "TotalNotNull")
        .withColumn("PercentagePass", round(col("Pass") / col("Total") * 100, 4).cast(StringType))
        .withColumn("PercentageFail", round(col("Fail") / col("Total") * 100, 4).cast(StringType))
        .withColumn("CheckType", lit("Completeness"))
        .withColumn("DateChecked", current_date())
        .select("LogicalAttribute", "Pass", "Fail", "Total", "PercentagePass", "PercentageFail", "CheckType", "DateChecked")

        passFailReportDF
    }
}
