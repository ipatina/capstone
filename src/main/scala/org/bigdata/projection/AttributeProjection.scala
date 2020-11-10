package org.bigdata.projection

import org.apache.spark.sql.DataFrame

trait AttributeProjection {
  /**
   * Build a projection for marketing analysis.
   * Schema for resulting dataframe:
   *   purchaseId:   string
   *   purchaseTime: timestamp
   *   billingCost:  double
   *   isConfirmed:  boolean
   *   sessionId:    string
   *   campaignId:   string
   *   channelId:    string
   *
   * @param clickstream input dataframe with app click events
   * @param purchaseDetails input dataframe with purchase details
   * @return joined and transformed dataframe of input dataframes
   */
  def getProjection(clickstream: DataFrame, purchaseDetails: DataFrame): DataFrame
}
