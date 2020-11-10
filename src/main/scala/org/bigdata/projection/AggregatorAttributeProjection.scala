package org.bigdata.projection

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.bigdata.utils.AttributeUtils._
import org.bigdata.utils.ColumnConstants._

class AggregatorAttributeProjection(spark: SparkSession) extends AttributeProjection {

  import AggregatorAttributeProjection._
  import spark.implicits._

  override def getProjection(clickstream: DataFrame, purchaseDetails: DataFrame): DataFrame = {
    val cleaned = cleanAttributesColumn(clickstream)
    val mapped = cleaned
      .resolveAttributes
      .mapToRawSchema(spark)

    val aggColumn = new AttributeAggregator(spark).toColumn.as(RESULT).as[Seq[Out]]
    val aggregated = mapped.groupByKey(_.userId).agg(aggColumn)

    transformAggregatedResult(aggregated)
      .join(purchaseDetails, Seq(PURCHASE_ID), LEFT_OUTER_JOIN)
  }
}

object AggregatorAttributeProjection {
  private val RESULT = "result"

  def transformAggregatedResult(result: Dataset[(String, Seq[Out])]): DataFrame =
    result.withColumn(RESULT, explode(col(RESULT)))
      .select(col(RESULT)(SESSION_ID).as(SESSION_ID),
        col(RESULT)(PURCHASE_ID).as(PURCHASE_ID),
        col(RESULT)(CAMPAIGN_ID).as(CAMPAIGN_ID),
        col(RESULT)(CHANNEL_ID).as(CHANNEL_ID))

  implicit class DataframeImplicits(df: DataFrame) {

    def resolveAttributes: DataFrame =
      df.filter(col(ATTRIBUTES).isNotNull)
        .withColumn(PURCHASE_STRUCT_NAME, when(col(EVENT_TYPE) === PURCHASE, purchaseJson))
        .withColumn(CAMPAIGN_STRUCT_NAME, when(col(EVENT_TYPE) === APP_OPEN, campaignJson))
        .withColumn(PURCHASE_ID, col(PURCHASE_STRUCT_NAME)(PURCHASE_ID_JSON_FIELD))
        .withColumn(CAMPAIGN_ID, col(CAMPAIGN_STRUCT_NAME)(CAMPAIGN_ID_JSON_FIELD))
        .withColumn(CHANNEL_ID, col(CAMPAIGN_STRUCT_NAME)(CHANNEL_ID_JSON_FIELD))
        .drop(PURCHASE_STRUCT_NAME, CAMPAIGN_STRUCT_NAME, ATTRIBUTES)

    def mapToRawSchema(spark: SparkSession): Dataset[RawSchema] = {
      import spark.implicits._
      df.map(row => {
        val userId = row.getString(row.fieldIndex(USER_ID))
        val eventId = row.getString(row.fieldIndex(EVENT_ID))
        val eventTime = row.getTimestamp(row.fieldIndex(EVENT_TIME))
        val purchaseId = Option(row.getString(row.fieldIndex(PURCHASE_ID)))
        val campaignId = Option(row.getString(row.fieldIndex(CAMPAIGN_ID)))
        val channelId = Option(row.getString(row.fieldIndex(CHANNEL_ID)))
        RawSchema(userId, eventId, eventTime, purchaseId, campaignId, channelId)
      })
    }
  }

  def apply(spark: SparkSession): AggregatorAttributeProjection = new AggregatorAttributeProjection(spark)
}