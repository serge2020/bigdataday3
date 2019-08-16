package com.accenture.bootcamp

import java.io.File

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class UtilsSuite extends FunSuite with Matchers with BeforeAndAfterAll with SparkSupport {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)


  override def beforeAll(): Unit = {
    val cwd = new File(".")
      .getAbsolutePath
      .replaceAll("\\\\", "/")
      .replaceAll("/\\.$", "")

    val hadoopHomeDir = new File(cwd).toURI.toString
    sys.props += "hadoop.home.dir" -> cwd
  }

  override def afterAll(): Unit = {
    sc.stop()
  }

  test("Read SacramentocrimeJanuary2006 correctly") {
    val path = Utils.filePath("SacramentocrimeJanuary2006.csv")
    val rdd = sc.textFile(path)
    val count = rdd.count()
    assert(rdd.count() == 7584)
  }

  test("Read ucr_ncic_codes correctly") {
    val path = Utils.filePath("ucr_ncic_codes.tsv")
    val rdd = sc.textFile(path)
    val count = rdd.count()
    assert(rdd.count() == 563)
  }

}
