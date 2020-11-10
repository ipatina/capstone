package org.bigdata.utils

import org.apache.spark.sql.functions.{col, from_json, regexp_replace}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame}
import org.bigdata.utils.ColumnConstants.ATTRIBUTES

object AttributeUtils {

  val CAMPAIGN_ID_JSON_FIELD = "campaign_id"
  val CHANNEL_ID_JSON_FIELD = "channel_id"
  val PURCHASE_ID_JSON_FIELD = "purchase_id"

  val CAMPAIGN_STRUCT_NAME = "campaignStruct"
  val PURCHASE_STRUCT_NAME = "purchaseStruct"

  /**
   * StructType for attributes json for 'app_open' event type.
   * Example: {"campaign_id": "cmp2",  "channel_id": "Yandex Ads"}
   */
  private val campaignStruct = StructType(
    Seq(
      StructField(CAMPAIGN_ID_JSON_FIELD, StringType),
      StructField(CHANNEL_ID_JSON_FIELD, StringType)
    )
  )

  /**
   * StructType for attributes json for 'purchase' event type.
   * Example: {"purchase_id": "p3"}
   */
  private val purchaseStruct = StructType(
    Seq(
      StructField(PURCHASE_ID_JSON_FIELD, StringType)
    )
  )

  val campaignJson: Column = from_json(col(ATTRIBUTES), campaignStruct)
  val purchaseJson: Column = from_json(col(ATTRIBUTES), purchaseStruct)

  /**
   * Replace double braces '{{' '}}' with a single brace '{', '}'
   * and replace incorrect quotation mark in attributes column to make this column a parsable json.
   */
  def cleanAttributesColumn(df: DataFrame): DataFrame =
    df.withColumn(ATTRIBUTES, regexp_replace(col(ATTRIBUTES), "[{][{]", "{"))
      .withColumn(ATTRIBUTES, regexp_replace(col(ATTRIBUTES), "[}][}]", "}"))
      .withColumn(ATTRIBUTES, regexp_replace(col(ATTRIBUTES), "[“]", "\""))

}
