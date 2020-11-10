package org.bigdata.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadUtils {

  val CLICKSTREAM_SAMPLE_PATH = "src/main/resources/clickstream_sample.tsv"
  val PURCHASES_SAMPLE_PATH = "src/main/resources/purchases_sample.tsv"
  val TEST_PATH = "src/test/resources/test_result.tsv"

  private val tsvOptions = Map(
    "header" -> "true",
    "inferSchema" -> "true",
    "delimiter" -> "\t"
  )

  def readTsv(spark: SparkSession, path: String): DataFrame =
    spark.read.options(tsvOptions).csv(path)

}
