package org.bigdata
import org.apache.spark.sql.SparkSession
import org.bigdata.projection.AttributeProjection
import org.bigdata.utils.IOUtils._
import org.bigdata.utils.ConfigUtils

class BuildProjectionAction(impl: AttributeProjection) extends Action {
  override def doAction()(implicit spark: SparkSession): Unit = {
    val conf = ConfigUtils.getConfig()

    val clickstream = readTsv(conf.clickstreamSamplePath, conf.tsvOptions)
    val purchaseDetails = readTsv(conf.purchasesSamplePath, conf.tsvOptions)

    val projection = impl.getProjection(clickstream, purchaseDetails)

    writeTsv(projection, conf.attributeProjectionPath, conf.tsvOptions)
  }
}