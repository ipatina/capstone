package org.bigdata.projection

import java.sql.Timestamp

case class RawSchema(userId: String,
                     eventId: String,
                     eventTime: Timestamp,
                     purchaseId: Option[String],
                     campaignId: Option[String],
                     channelId: Option[String])

case class Out(sessionId: String,
               purchaseId: Option[String],
               campaignId: String,
               channelId: String)


