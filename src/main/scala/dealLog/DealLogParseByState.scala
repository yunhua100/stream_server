package dealLog

import com.alibaba.fastjson.JSONObject
import dealLog.model.Constants
import dealLog.resource.{ConfLoad, Parse}
import dealLog.util.ElasticClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.Map
import logger.IcopLoggerFactory

object DealLogParseByState extends Logging {

  private val logger = IcopLoggerFactory.getLogger(DealLogParseByState.getClass)
  private val brokers = ConfLoad.getInstance().getPropValue("brokers")
  private val topic = ConfLoad.getInstance().getPropValue("topic")
  private val groupId = ConfLoad.getInstance().getPropValue("groupId")
  private val checkpointDirectory = ConfLoad.getInstance().getPropValue("checkpointDirectory")
  private val durationTimeSeconds = ConfLoad.getInstance().getPropIntValue("durationTimeSeconds")
  private val maxRatePerPartition = ConfLoad.getInstance().getPropValue("maxRatePerPartition")
  private val appName = ConfLoad.getInstance().getPropValue("appName")
  private val sparkMaster = ConfLoad.getInstance().getPropValue("sparkMaster")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName)
    if(sparkMaster != null && sparkMaster != "")
      conf.setMaster(sparkMaster)
    conf.set("spark.extraListeners", "dealLog.listener.StreamListener")
    conf.set("spark.streaming.kafka.maxRatePerPartition",maxRatePerPartition)
    val sc = new SparkContext(conf)
    //sc.setLogLevel("WARN")

    val createContextFunc = () => {
      val ssc = new StreamingContext(sc, Durations.seconds(durationTimeSeconds))
      ssc.checkpoint(checkpointDirectory)

      val topicSet = topic.split(",").toSet
      val lines: DStream[ConsumerRecord[Any, Any]] = KafkaUtils.createDirectStream(
        ssc,
        PreferConsistent,
        Subscribe(topicSet, getKafkaParam(brokers, groupId))
      )
      logger.info("kafka consumer success!")

      val parseLines = lines.mapPartitions{ x => {
        var correctResult = List[JSONObject]()
        var errorResult = List[JSONObject]()
        while(x.hasNext){
          val line = x.next()
          val data = Parse.parse(line.value().toString)
          if(data != null){
            data.put(Constants.TOPIC, line.topic())
            val parseStatus = data.getInteger(Constants.PARSE_STATUS)
            if(parseStatus != null && parseStatus == 1){
              correctResult = correctResult :+ data
            }else{
              errorResult = errorResult :+ data
            }
            data.remove(Constants.PARSE_STATUS)
          }
        }
        ElasticClient.getInstance().insertBulkCorrect(correctResult)
        ElasticClient.getInstance().insertBulkError(errorResult)
        correctResult.iterator
      }}

      DealLogMerge.merge(parseLines.map(x=>x.toJSONString))
      //mapRdd.checkpoint(Durations.seconds(10 * durationTimeSeconds))
      saveOffset(lines)
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointDirectory, createContextFunc)
    ssc.start
    ssc.awaitTermination
  }

  def getKafkaParam(brokers: String, groupId: String): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      //"auto.offset.reset" -> "latest",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }

  def saveOffset(dStream: DStream[ConsumerRecord[Any, Any]]): Unit = {
    dStream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
  }
}
