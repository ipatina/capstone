package org.bigdata.projection

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.bigdata.utils.ReadUtils

object AttributeProjectionTestCases {

  def testGetProjection(ap: AttributeProjection, spark: SparkSession): Unit = {
    import spark.implicits._

    val clickstream = ReadUtils.readTsv(spark, ReadUtils.CLICKSTREAM_SAMPLE_PATH)
    val purchaseDetails = ReadUtils.readTsv(spark, ReadUtils.PURCHASES_SAMPLE_PATH)

    val result = ap.getProjection(clickstream, purchaseDetails)
    val expected = ReadUtils.readTsv(spark, ReadUtils.TEST_PATH)

    val resultCollected = result
      .as[(String, String, String, String, Timestamp, Option[Double], Option[Boolean])].collect()
    val expectedCollected = expected
      .as[(String, String, String, String, Timestamp, Option[Double], Option[Boolean])].collect()

    assert(expectedCollected.sortWith(_._2 > _._2).sameElements(resultCollected.sortWith(_._2 > _._2)))
  }
}
