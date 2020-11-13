package org.bigdata.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

object IOUtils {

  def readTsv(path: String, options: Map[String, String])
             (implicit spark: SparkSession): DataFrame =
    spark.read.options(options).csv(path)

  def writeTsv(df: DataFrame, path: String, options: Map[String, String]): Unit =
    df.write.options(options).csv(path)
}
