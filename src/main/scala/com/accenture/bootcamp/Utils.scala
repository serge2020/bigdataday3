package com.accenture.bootcamp

import java.io.File

import org.apache.log4j.{Level, LogManager}

object Utils {

  /**
    * Execution time measurement for code blocks
    * @param block
    * @tparam R
    * @return
    */
  def time[R](block: => R): (Long, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    (t1 - t0) -> result
  }

  def filePath(path: String): String = {
    new File(s"src/main/resources/$path").toURI.toString
  }

  def configureLogger: Unit = {
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.INFO)

    val orgLogger = LogManager.getLogger("org")
    logger.setLevel(Level.OFF)
  }
}
