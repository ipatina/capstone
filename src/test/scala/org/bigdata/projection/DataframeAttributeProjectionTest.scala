package org.bigdata.projection

import java.sql.Timestamp

import org.bigdata.TestBase
import org.bigdata.projection.AttributeProjectionTestCases.testGetProjection
import org.bigdata.projection.DataframeAttributeProjection._
import org.bigdata.utils.ColumnConstants._
import org.joda.time.DateTime
import org.junit.Test

class DataframeAttributeProjectionTest extends TestBase {

  import DataframeAttributeProjection.DataframeImplicits
  import spark.implicits._

  @Test
  def test_getProjection(): Unit = {
    testGetProjection(DataframeAttributeProjection(), spark)
  }

  @Test
  def test_joinCampaignsToPurchases(): Unit = {
    val campaignsDf = Seq(
      ("u1",
        new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-01T0:30:00").getMillis)),
      ("u2",
        new Timestamp(DateTime.parse("2019-01-02T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-02T0:04:00").getMillis))
    ).toDF(USER_ID, APP_OPEN_TIME, APP_CLOSE_TIME)

    val purchasesDf = Seq(("u1", new Timestamp(DateTime.parse("2019-01-01T0:10:00").getMillis)))
      .toDF(PURCHASE_USER_ID, EVENT_TIME)

    val result = joinCampaignsToPurchases(campaignsDf, purchasesDf)
      .as[(String, Timestamp, Timestamp, String, Timestamp)].collect()

    val expected = Array(
      ("u1",
        new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-01T0:30:00").getMillis),
        "u1", new Timestamp(DateTime.parse("2019-01-01T0:10:00").getMillis)),
      ("u2",
        new Timestamp(DateTime.parse("2019-01-02T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-02T0:04:00").getMillis),
        null, null)
    )

    assert(expected.sameElements(result))
  }

  @Test
  def test_prepareCampaignForJoin(): Unit = {
    val testDf = Seq(
      ("u1", "u1_e1", new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis), "app_open", "attributes1"),
      ("u1", "u1_e2", new Timestamp(DateTime.parse("2019-01-01T0:03:00").getMillis), "purchase", "attributes2"),
      ("u1", "u1_e3", new Timestamp(DateTime.parse("2019-01-01T0:04:00").getMillis), "app_close", null),
      ("u1", "u1_e4", new Timestamp(DateTime.parse("2019-01-02T0:00:00").getMillis), "app_open", "attributes3"),
      ("u1", "u1_e5", new Timestamp(DateTime.parse("2019-01-02T0:04:00").getMillis), "app_close", null),
      ("u2", "u2_e1", new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis), "app_open", "attributes4"),
      ("u2", "u2_e2", new Timestamp(DateTime.parse("2019-01-01T0:01:00").getMillis), "purchase", "attributes5"),
      ("u2", "u2_e3", new Timestamp(DateTime.parse("2019-01-01T0:02:00").getMillis), "app_close", null),
      ("u2", "u2_e4", new Timestamp(DateTime.parse("2019-01-01T1:11:11").getMillis), "app_open", "attributes6"),
      ("u2", "u2_e5", new Timestamp(DateTime.parse("2019-01-01T1:12:00").getMillis), "purchase", "attributes7"),
      ("u2", "u2_e6", new Timestamp(DateTime.parse("2019-01-01T1:13:00").getMillis), "purchase", "attributes8"),
      ("u2", "u2_e7", new Timestamp(DateTime.parse("2019-01-01T1:14:30").getMillis), "app_close", null)
    ).toDF(USER_ID, EVENT_ID, EVENT_TIME, EVENT_TYPE, ATTRIBUTES)

    val result = testDf.prepareCampaignForJoin
      .as[(String, String, String, Timestamp, Timestamp)].collect()

    val expected = Array(
      ("u1", "u1_e1", "attributes1",
        new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-01T0:04:00").getMillis)),

      ("u1", "u1_e4", "attributes3",
        new Timestamp(DateTime.parse("2019-01-02T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-02T0:04:00").getMillis)),

      ("u2", "u2_e1", "attributes4",
        new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis),
        new Timestamp(DateTime.parse("2019-01-01T0:02:00").getMillis)),

      ("u2", "u2_e4", "attributes6",
        new Timestamp(DateTime.parse("2019-01-01T1:11:11").getMillis),
        new Timestamp(DateTime.parse("2019-01-01T1:14:30").getMillis))
    )

    assert(expected.sortWith(_._2 > _._2).sameElements(result.sortWith(_._2 > _._2)))
  }

  @Test
  def test_preparePurchaseForJoin(): Unit = {
    val testDf = Seq(
      ("u1", new Timestamp(DateTime.parse("2019-01-01T0:00:00").getMillis), "app_open", "attributes1"),
      ("u2", new Timestamp(DateTime.parse("2019-01-01T0:03:00").getMillis), "purchase", "attributes2")
    ).toDF(USER_ID, EVENT_TIME, EVENT_TYPE, ATTRIBUTES)

    val result = testDf.preparePurchaseForJoin.as[(String, Timestamp, String)].collect()

    val expected = Array(("u2", new Timestamp(DateTime.parse("2019-01-01T0:03:00").getMillis), "attributes2"))

    assert(expected.sameElements(result))
  }

  @Test
  def test_resolveCampaignAttributes(): Unit = {
    val testDf = Seq("{\"campaign_id\": \"cmp2\",  \"channel_id\": \"Yandex Ads\"}").toDF(ATTRIBUTES)

    val result = testDf.resolveCampaignAttributes.as[(String, String)].collect()
    val expected = Array(("cmp2", "Yandex Ads"))

    assert(expected.sameElements(result))
  }

  @Test
  def test_resolvePurchaseAttributes(): Unit = {
    val testDf = Seq("{\"purchase_id\": \"p3\"}").toDF(ATTRIBUTES)

    val result = testDf.resolvePurchaseAttributes.as[String].collect()
    val expected = Array("p3")

    assert(expected.sameElements(result))
  }

  @Test
  def test_addSessionIdAndSelect(): Unit = {
    val testDf = Seq(("p1", "u1_e1", "c1", "c2"), (null, "u1_e2", "c3", "c4"))
      .toDF(PURCHASE_ID, EVENT_ID, CAMPAIGN_ID, CHANNEL_ID)

    val result = testDf.addSessionIdAndSelect.as[(String, String, String, String)].collect()
    val expected = Array(("p1", "u1_e1_p1", "c1", "c2"), (null, "u1_e2", "c3", "c4"))

    assert(expected.sameElements(result))
  }
}
