package org.bigdata.projection

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.bigdata.utils.ConfigUtils.Config
import org.bigdata.utils.IOUtils

object AttributeProjectionTestCases {

  def testGetProjection(ap: AttributeProjection, conf: Config)(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val clickstream = IOUtils.readTsv(conf.clickstreamSamplePath, conf.tsvOptions)
    val purchaseDetails = IOUtils.readTsv(conf.purchasesSamplePath, conf.tsvOptions)

    val result = ap.getProjection(clickstream, purchaseDetails)
    val expected = IOUtils.readTsv(conf.attributeProjectionPath, conf.tsvOptions)

    val resultCollected = result
      .as[(String, String, String, String, Timestamp, Option[Double], Option[Boolean])].collect()
    val expectedCollected = expected
      .as[(String, String, String, String, Timestamp, Option[Double], Option[Boolean])].collect()

    assert(expectedCollected.sortWith(_._2 > _._2).sameElements(resultCollected.sortWith(_._2 > _._2)))
  }
}
