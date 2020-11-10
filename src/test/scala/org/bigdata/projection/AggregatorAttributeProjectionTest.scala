package org.bigdata.projection

import org.bigdata.TestBase
import org.bigdata.projection.AttributeProjectionTestCases.testGetProjection
import org.bigdata.utils.ColumnConstants.{ATTRIBUTES, EVENT_TYPE}
import org.junit.Test

class AggregatorAttributeProjectionTest extends TestBase {

  import AggregatorAttributeProjection.DataframeImplicits
  import spark.implicits._

  @Test
  def test_getProjection(): Unit = {
    testGetProjection(AggregatorAttributeProjection(spark), spark)
  }

  @Test
  def test_resolveAttributes(): Unit = {
    val testDf = Seq(
      ("{\"campaign_id\": \"cmp2\",  \"channel_id\": \"Yandex Ads\"}", "app_open"),
      ("{\"purchase_id\": \"p3\"}", "purchase")).toDF(ATTRIBUTES, EVENT_TYPE)

    val result = testDf.resolveAttributes.as[(String, String, String, String)].collect()
    val expected = Array(("app_open", null, "cmp2", "Yandex Ads"), ("purchase", "p3", null, null))

    assert(expected.sameElements(result))
  }
}
