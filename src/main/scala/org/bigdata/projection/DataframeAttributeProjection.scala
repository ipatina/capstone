package org.bigdata.projection

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.bigdata.utils.AttributeUtils._
import org.bigdata.utils.ColumnConstants._

class DataframeAttributeProjection extends AttributeProjection {

  import DataframeAttributeProjection._

  override def getProjection(clickstream: DataFrame, purchaseDetails: DataFrame): DataFrame = {
    val cleaned = cleanAttributesColumn(clickstream)

    val campaigns = cleaned
      .prepareCampaignForJoin
      .resolveCampaignAttributes

    val purchases = cleaned
      .preparePurchaseForJoin
      .resolvePurchaseAttributes

    joinCampaignsToPurchases(campaigns, purchases)
      .addSessionIdAndSelect
      .join(purchaseDetails, Seq(PURCHASE_ID), LEFT_OUTER_JOIN)
  }
}

object DataframeAttributeProjection {

  private[projection] val APP_OPEN_TIME = "appOpenTime"
  private[projection] val APP_CLOSE_TIME = "appCloseTime"
  private[projection] val PURCHASE_USER_ID = "purchaseUserId"

  private[projection] def joinCampaignsToPurchases(campaigns: DataFrame, purchases: DataFrame): DataFrame = {
    val joinExpression = (col(USER_ID) === col(PURCHASE_USER_ID)) and
      col(EVENT_TIME).between(col(APP_OPEN_TIME), col(APP_CLOSE_TIME))

    campaigns.join(purchases, joinExpression, LEFT_OUTER_JOIN)
  }

  private[projection] implicit class DataframeImplicits(df: DataFrame) {

    /**
     * Map each app_open event to app_close event,
     * eventTime columns for mapped events will represent a session duration
     */
    def prepareCampaignForJoin: DataFrame = {
      val appOpen = df.filter(col(EVENT_TYPE) === APP_OPEN)
        .withColumnRenamed(EVENT_TIME, APP_OPEN_TIME)
      val appClose = df.filter(col(EVENT_TYPE) === APP_CLOSE)
        .select(col(USER_ID), col(EVENT_TIME).as(APP_CLOSE_TIME))

      appOpen.join(appClose, USER_ID)
        .filter(appClose(APP_CLOSE_TIME) > appOpen(APP_OPEN_TIME))
        .groupBy(USER_ID, EVENT_ID, ATTRIBUTES, APP_OPEN_TIME)
        .agg(min(APP_CLOSE_TIME).as(APP_CLOSE_TIME))
    }

    def preparePurchaseForJoin: DataFrame =
      df.filter(col(EVENT_TYPE) === PURCHASE)
        .select(col(USER_ID).as(PURCHASE_USER_ID), col(EVENT_TIME), col(ATTRIBUTES))

    /**
     * Parse json from attributes to two additional columns: campaignId and channelId
     */
    def resolveCampaignAttributes: DataFrame = {
      df.withColumn(CAMPAIGN_STRUCT_NAME, campaignJson)
        .withColumn(CAMPAIGN_ID, col(CAMPAIGN_STRUCT_NAME)(CAMPAIGN_ID_JSON_FIELD))
        .withColumn(CHANNEL_ID, col(CAMPAIGN_STRUCT_NAME)(CHANNEL_ID_JSON_FIELD))
        .drop(CAMPAIGN_STRUCT_NAME, ATTRIBUTES)
    }

    /**
     * Parse json from attributes to purchaseId column
     */
    def resolvePurchaseAttributes: DataFrame = {
      df.withColumn(PURCHASE_STRUCT_NAME, purchaseJson)
        .withColumn(PURCHASE_ID, col(PURCHASE_STRUCT_NAME)(PURCHASE_ID_JSON_FIELD))
        .drop(PURCHASE_STRUCT_NAME, ATTRIBUTES)
    }

    def addSessionIdAndSelect: DataFrame = {
      val sessionIdColumn = when(col(PURCHASE_ID).isNotNull,
        concat(col(EVENT_ID), lit("_"), col(PURCHASE_ID))).otherwise(col(EVENT_ID))

      df.withColumn(SESSION_ID, sessionIdColumn)
        .select(PURCHASE_ID, SESSION_ID, CAMPAIGN_ID, CHANNEL_ID)
    }
  }

  def apply(): DataframeAttributeProjection = new DataframeAttributeProjection()
}