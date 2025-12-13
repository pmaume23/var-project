package com.dq.pipeline.utils

import io.github.cdimascio.dotenv.Dotenv
import org.slf4j.{Logger, LoggerFactory}

/**
 * ConfigLoader loads environment variables from .env file before initializing the application config
 */
object ConfigLoader {
  
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  
  /**
   * Load .env file and set environment variables as system properties
   * Call this BEFORE accessing any configuration
   */
  def loadEnv(): Unit = {
    try {
      val dotenv = Dotenv.configure()
        .ignoreIfMissing()  // Don't fail if .env doesn't exist
        .load()
      
      logger.info("Loading environment variables from .env file...")
      
      // Set each .env variable as a system property so Config can read it
      dotenv.entries().forEach { entry =>
        System.setProperty(entry.getKey, entry.getValue)
        logger.debug(s"Loaded env variable: ${entry.getKey}")
      }
      
      logger.info(s"Successfully loaded ${dotenv.entries().size()} environment variables")
    } catch {
      case e: Exception =>
        logger.warn("Failed to load .env file, using default configuration", e)
    }
  }
}
