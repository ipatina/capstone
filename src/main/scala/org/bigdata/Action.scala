package org.bigdata

import org.apache.spark.sql.SparkSession

trait Action {
  def doAction()(implicit spark: SparkSession): Unit
}
