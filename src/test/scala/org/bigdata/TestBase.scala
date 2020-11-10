package org.bigdata

import org.apache.spark.sql.SparkSession
import org.bigdata.TestBase.createSparkSession

class TestBase {
  lazy val spark: SparkSession = createSparkSession
}

object TestBase {
  private def createSparkSession: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone tests")
      .master("local")
      .getOrCreate()
}