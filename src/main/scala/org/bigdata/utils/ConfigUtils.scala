package org.bigdata.utils

import java.io.IOException
import java.util

import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.io.Source


object ConfigUtils {

  val BUILD_PROJECTION = "build_projection"
  val BUILD_STATISTICS = "build_statistics"
  val AGGREGATOR_IMPL = "aggregator"
  val SQL_IMPL = "sql"

  val CONFIG_PATH_CONF = "spark.capstone.config.path"
  val ACTION_TYPE_CONF = "spark.capstone.action.type"
  val ACTION_IMPLEMENTATION_CONF = "spark.capstone.action.implementation"

  case class Config(clickstreamSamplePath: String,
                    purchasesSamplePath: String,
                    attributeProjectionPath: String,
                    statisticsRootPath: String,
                    tsvOptions: Map[String, String])

  def getConfig(fromResources: Boolean = false)(implicit spark: SparkSession): Config = {
    val configPath = getSparkConf(CONFIG_PATH_CONF)
    val str = readYamlToString(configPath, fromResources)
    val yaml = new Yaml()
    val map = yaml.load[util.Map[String, Any]](str)

    parseYamlFromMap(map)
  }

  def getSparkConf(name: String, defaultVal: Option[String] = None)
                  (implicit spark: SparkSession): String = {
    spark.conf.getOption(name) match {
      case Some(value) => value
      case None => defaultVal.getOrElse(throw new RuntimeException(s"Please specify $name spark conf!"))
    }
  }

  private def parseYamlFromMap(map: util.Map[String, Any]): Config = {
    val tsvOptions = map.get("tsvOptions").asInstanceOf[util.Map[String, Any]]
    val stringOptions = for ((k, v) <- tsvOptions.asScala) yield k -> v.toString

    Config(
      map.get("clickstreamSamplePath").toString,
      map.get("purchasesSamplePath").toString,
      map.get("attributeProjectionPath").toString,
      map.get("statisticsRootPath").toString,
      stringOptions.toMap
    )
  }

  private def readYamlToString(path: String, fromResources: Boolean) = {
    val configSource = if(fromResources) Source.fromResource(path) else Source.fromFile(path)
    try {
      configSource.mkString
    } catch {
      case ex: IOException =>
        throw new RuntimeException(s"Failed to read yaml from path: $path \n$ex", ex)
    } finally {
      configSource.close()
    }
  }
}
