package com.`var`.pipeline

import com.`var`.pipeline.nodes.VarSvarNode
import com.dq.pipeline.utils.ConfigLoader
import org.slf4j.{Logger, LoggerFactory}
import scala.io.StdIn

object VarSvarMain {

    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def main(args: Array[String]): Unit = {
        logger.info("Starting VaR SVar Application...")

        // Load env (e.g., DB_PASSWORD) before any config is resolved
        ConfigLoader.loadEnv()

        val varSvarNode = new VarSvarNode()

        //Display Combined Var And SVar
        val combinedVarSVarDF = varSvarNode.combinedVarSVarDF
        logger.info("=== Combined VaR and SVar DataFrame ===")
        combinedVarSVarDF.show(50, truncate = false)
        logger.info(s"Total records in Combined VaR and SVar DataFrame: ${combinedVarSVarDF.count()}")

        // Persist combined results to HDFS as Parquet
        // Allow overriding the output path via the OUTPUT_PATH environment variable.
        // If not set, default to a local filesystem directory under the project working directory
        val outputPath = Option(System.getenv("OUTPUT_PATH")).filter(_.nonEmpty)
          .getOrElse(s"file://${new java.io.File(".").getCanonicalPath}/varsvar/combined_parquet")

        logger.info(s"Writing combined VaR/SVaR to $outputPath (overwrite mode)...")
                combinedVarSVarDF
                    .write
                    .mode("overwrite")
                    .partitionBy("year", "quarter")
                    .parquet(outputPath)
        logger.info("Write complete.")

        // Optionally keep the driver running so the Spark UI remains available
        // Set environment variable KEEP_UI_ALIVE=true to enable. Press ENTER to exit.
        val keepUi = Option(System.getenv("KEEP_UI_ALIVE")).exists(_.toLowerCase == "true")
        if (keepUi) {
          logger.info("KEEP_UI_ALIVE=true: keeping application alive so Spark UI stays up. Press ENTER to stop.")
          StdIn.readLine()
          logger.info("Exiting after user input; SparkContext will stop and Spark UI will exit.")
        }

    }

}
