package com.`var`.pipeline

import com.`var`.pipeline.nodes.VarSvarNode
import com.dq.pipeline.utils.ConfigLoader
import org.slf4j.{Logger, LoggerFactory}

object VarSvarMain {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        logger.info("Starting VaR SVar Application...")

        // Load env (e.g., DB_PASSWORD) before any config is resolved
        ConfigLoader.loadEnv()

        val varSvarNode = new VarSvarNode()

        // Display rolling time-series VaR
        val rollingVarDF = varSvarNode.rollingVarDF
        logger.info("=== Rolling VaR Time Series (252-day window) ===")
        rollingVarDF.show(20, truncate = false)
        logger.info(s"Total time periods with rolling VaR: ${rollingVarDF.count()}")

        // Display rolling stressed VaR
        val rollingStressedVarDF = varSvarNode.rollingStressedVarDF
        logger.info("=== Rolling Stressed VaR (2008, 2020 crisis periods) ===")
        rollingStressedVarDF.show(20, truncate = false)
        logger.info(s"Total time periods with rolling stressed VaR: ${rollingStressedVarDF.count()}")

        //Display Combined Var And SVar
        val combinedVarSVarDF = varSvarNode.combinedVarSVarDF
        logger.info("=== Combined VaR and SVar DataFrame ===")
        combinedVarSVarDF.show(50, truncate = false)
        logger.info(s"Total records in Combined VaR and SVar DataFrame: ${combinedVarSVarDF.count()}")

        // Persist combined results to HDFS as Parquet
        val outputPath = "hdfs://localhost:9000/user/physiwellmaume/varsvar/combined_parquet"
        logger.info(s"Writing combined VaR/SVaR to $outputPath (overwrite mode)...")
                combinedVarSVarDF
                    .write
                    .mode("overwrite")
                    .partitionBy("year", "quarter")
                    .parquet(outputPath)
        logger.info("Write complete.")
    }
  
}
