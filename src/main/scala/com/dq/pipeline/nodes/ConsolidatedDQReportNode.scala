package com.dq.pipeline.Nodes

import com.dq.pipeline.Helpers.LoadSP500Data
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

/**
 * ConsolidatedDQReportNode consolidates all data quality checks into a single report.
 * Currently includes:
 * - Completeness Check
 * - Uniqueness Check
 * - Validity Check
 * - Conformity Check
 */
class ConsolidatedDQReportNode(sp500DataDF: DataFrame = LoadSP500Data.sp500DataDF) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Generate consolidated data quality report
   * @return DataFrame with all DQ check results
   */
  def generateReport(): DataFrame = {
    logger.info("Generating consolidated data quality report")

    // Run completeness check
    val completenessCheckNode = new CompletenessCheckNode(sp500DataDF)
    val completenessReport = completenessCheckNode.completenessReportDF()
    
    // Run uniqueness check
    val uniquenessCheckNode = new UniquenessCheckNode(sp500DataDF)
    val uniquenessReport = uniquenessCheckNode.uniquenessReportDF()
    
    // Run validity check
    val validityCheckNode = new ValidityCheckNode(sp500DataDF)
    val validityReport = validityCheckNode.validityReportDF()
    
    // Run conformity check
    val conformityCheckNode = new ConformityCheckNode(sp500DataDF)
    val conformityReport = conformityCheckNode.conformityReportDF()
    
    // Consolidate all reports using union
    val consolidatedReport = completenessReport
      .union(uniquenessReport)
      .union(validityReport)
      .union(conformityReport)

    logger.info("Consolidated data quality report prepared")
    
    consolidatedReport
  }
}
