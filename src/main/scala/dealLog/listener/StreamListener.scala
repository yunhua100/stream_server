package dealLog.listener

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}

class StreamListener(val sparkConf: SparkConf) extends SparkListener with Logging{
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appId = applicationStart.appId
    logInfo("DeallogParse App start " + appId.getOrElse("no appId"))
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logInfo("DeallogParse App end time " + applicationEnd.time)
  }
}
