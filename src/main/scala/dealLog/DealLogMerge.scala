package dealLog

import com.google.gson.reflect.TypeToken
import com.google.gson.{Gson, JsonObject, JsonParser}
import dealLog.model.DealLogMsg
import dealLog.resource.{CalcResource, ConfLoad, InfluxDBTemplate}
import dealLog.util.{ElasticClient, TimeUtil}
import logger.IcopLoggerFactory
import org.apache.spark.HashPartitioner
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, State, StateSpec}

object DealLogMerge extends Logging {
  private val logger = IcopLoggerFactory.getLogger(DealLogMerge.getClass)
  private val timeoutSeconds = ConfLoad.getInstance.getPropIntValue("timeoutSeconds")
  private val partitionNum = ConfLoad.getInstance.getPropIntValue("partitionNum")
  private val influxDBUrl = ConfLoad.getInstance.getPropValue("influxDBUrl")
  private val dbName = ConfLoad.getInstance.getPropValue("dbName")
  private val measurement = ConfLoad.getInstance.getPropValue("measurement")
  private val influxErrorIndex = ConfLoad.getInstance.getPropValue("influxErrorIndex")
  private val influxErrorType = ConfLoad.getInstance.getPropValue("influxErrorType")
  private val exKey = "exMsg"
  private val mapType = new TypeToken[java.util.Map[String,String]](){}.getType

  def merge(dStream: DStream[String]):Unit = {
    val mappingFunc = (key: String, newLog: Option[DealLogMsg], state: State[DealLogMsg]) => {
      var output: Option[DealLogMsg] = None
      if (key != exKey) {
        if (state.isTimingOut()) {
          logger.info("time out ==" + key + " data==" + CalcResource.gson.toJson(state.get()))

          val timeoutDm = state.get()
          timeoutDm.setFailProp(timeoutSeconds * 1000)
          output = Some(timeoutDm)
        } else {
          var dm = newLog.get
          if (state.exists()) {
            dm = state.get.matchDeal(dm)
            logger.info("state exist")

            if (dm.isCompleteDeal) {
              logger.info("found complete deal")
              dm.setSuccessProp
              output = Some(dm)
              state.remove()
            }
          } else {
            dm = dm.matchDeal(dm)
            state.update(dm)
            logger.info("state not exist")
          }
        }
      }
      output
    }

    val mapRdd = dStream
      .map(mapFunc = msg => {
        var dm: DealLogMsg = null
        try {
          dm = gson2Obj(msg)
          if (dm.gson2ObjIsRight()) {
            new Tuple2[String, DealLogMsg](dm.getIndetifier, dm)
          } else {
            logger.info("gson2ObjIsRight error")
            setErrorToInfluxDB(dm)
            new Tuple2[String, DealLogMsg](exKey, dm)
          }
        } catch {
          case e: Exception => {
            logger.error(e.getMessage)
            setErrorToInfluxDB(dm)
            new Tuple2[String, DealLogMsg](exKey, dm)
          }
        }
      })
      .mapWithState(StateSpec.function(mappingFunc)
        .partitioner(new HashPartitioner(partitionNum))
        .timeout(Durations.seconds(timeoutSeconds)))

    mapRdd.foreachRDD(rdd => {
      rdd.foreachPartition(r => {
        val influxDB = CalcResource.getInfluxDB(influxDBUrl, dbName)
        while (r.hasNext) {
          val data = r.next()
          if (!data.isEmpty) {
            logger.info("send to influxDB==" + new Gson().toJson(data.get))
            InfluxDBTemplate.insertData(influxDB, measurement, data.get)
          }
        }
      }
      )
    })
  }

  def setErrorToInfluxDB(dm:DealLogMsg) = {
    val dmMap:java.util.Map[String,String] = CalcResource.gson.fromJson(CalcResource.gson.toJson(dm),mapType)
    ElasticClient.getInstance().insertInfluxDBBulk(List(dmMap),influxErrorIndex + "-" + TimeUtil.getCurrTime,influxErrorType)
  }

  def gson2Obj(json:String):DealLogMsg = {
    val dm = CalcResource.gson.fromJson(json, classOf[DealLogMsg])
    dm.logSource = json

    val jo:JsonObject = new JsonParser().parse(json).getAsJsonObject
    dm.timestamp = jo.get("@timestamp").getAsString
    dm
  }
}
