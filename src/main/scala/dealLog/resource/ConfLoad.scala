package dealLog.resource

import java.util.Properties
import org.apache.spark.internal.Logging

object ConfLoad{
  private var confLoad: ConfLoad = _

  def getInstance(): ConfLoad = {
    if (confLoad == null) {
      synchronized {
        if (confLoad == null) {
          confLoad = new ConfLoad
        }
      }
    }
    confLoad
  }
}

class ConfLoad private extends Logging {
  private lazy val props: Properties = {
    val props = new Properties()
    val input = this.getClass.getResourceAsStream("/conf/calc-stream-conf.properties")
    props.load(input)
    input.close()
    props
  }

  private lazy val topicTypes: Map[String, String] = {
    var topicTypes: Map[String, String] = Map()
    val topics = props.getProperty("topic")
    for (i <- topics.split(",")) {
      topicTypes += (i -> getPropValue(i))
    }
    topicTypes
  }

  def getPropValue(key: String): String = {
    val v = props.getProperty(key)
    logInfo("calc stream conf: " + key + "=======" + v)
    v
  }

  def getPropIntValue(key: String): Int = {
    val v = java.lang.Integer.parseInt(props.getProperty(key))
    logInfo("calc stream conf: " + key + "=======" + v)
    v
  }

  def getTopicType(topic: String): String = {
    topicTypes.get(topic).orNull
  }

}




