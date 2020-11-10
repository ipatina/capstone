package org.bigdata.utils

object ColumnConstants {

  val LEFT_OUTER_JOIN = "left_outer"

  // Clickstream dataset column names
  val USER_ID = "userId"
  val EVENT_ID = "eventId"
  val EVENT_TIME = "eventTime"
  val EVENT_TYPE = "eventType"
  val ATTRIBUTES = "attributes"

  // Purchase details dataset column names
  val IS_CONFIRMED = "isConfirmed"
  val BILLING_COST = "billingCost"

  // Resulting dataset column names
  val SESSION_ID = "sessionId"
  val PURCHASE_ID = "purchaseId"
  val CAMPAIGN_ID = "campaignId"
  val CHANNEL_ID = "channelId"

  // Click event types
  val APP_OPEN = "app_open"
  val PURCHASE = "purchase"
  val APP_CLOSE = "app_close"

}
