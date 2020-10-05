package com.cohort.util

import com.cohort.conf.CohortConf
import org.apache.spark.sql.DataFrame
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConversions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Utils extends Logging {

  def selectColumns(conf: CohortConf, sourceKey: String, inputDf: DataFrame): DataFrame = {
    val fields          = getListFromConf(conf.selectColumnsConfigFile(), sourceKey).map(col)
    val outputDf        = inputDf.select(fields: _*)
    outputDf
  }

  def getListFromConf(configFileName: String, confKey: String): List[String] = {
    try {
      ConfigFactory.load(configFileName).getStringList(confKey).toList
    } catch {
      case e: Exception =>
        logError(s"*** Error parsing for $confKey as List[String] from $configFileName ***\n${e.getMessage}")
        List[String]()
    }
  }

  def pathGenerator(inputParentPath: String, datePrefix: String, processDate: String): String = {
    s"$inputParentPath/$datePrefix=$processDate/"
  }

  def dayAgoDateString(conf: CohortConf, dayAgo: Int): String = {
    val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val processDate: DateTime         = DateTime.parse(conf.processDate(), dateFormat)
    dateFormat.print(processDate.minusDays(dayAgo))
  }
}
