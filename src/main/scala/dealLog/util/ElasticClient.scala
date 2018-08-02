package dealLog.util

import java.net.InetAddress

import com.alibaba.fastjson.JSONObject
import dealLog.model.Constants
import dealLog.resource.ConfLoad
import org.apache.spark.internal.Logging
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}
import org.elasticsearch.transport.client.PreBuiltTransportClient

object ElasticClient{
  private var elasticClient : ElasticClient = _

  def getInstance() : ElasticClient = {
    if(elasticClient == null){
      synchronized{
        if(elasticClient == null){
          elasticClient = new ElasticClient
        }
      }
    }
    elasticClient
  }
}

class ElasticClient private extends Logging {
  private lazy val client: TransportClient = create()

  private def create(): TransportClient = {
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val  settings: Settings = Settings.builder.put("cluster.name", ConfLoad.getInstance().getPropValue("cluster.name")).put("client.transport.sniff", true).put("client.transport.ping_timeout", "60s").build
    val client : TransportClient = new PreBuiltTransportClient(settings)
    val nodes = ConfLoad.getInstance().getPropValue("es.nodes")
    val port = Integer.parseInt(ConfLoad.getInstance().getPropValue("es.port"))
    val nodeSplit = nodes.split(",")
    for(i <- nodeSplit){
      //client.addTransportAddresses(new TransportAddress(InetAddress.getByName(i), port)) //6.3版本适用
      client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName(i), port))
    }
    client
  }

  /*
  * 获取真正的客户端
  * */
  def get(): TransportClient = client

  /*
    * 解析成功数据批量插入es
   **/
  def insertBulkCorrect(list : List[JSONObject]) : Boolean = {
    try {
      val bulkRequestBuilder: BulkRequestBuilder = client.prepareBulk()
      for (i <- list.indices) {
        val data = list(i)
        val topic = data.getString(Constants.TOPIC)
        val esType = data.getString(Constants.LOG_TYPE)
        val esIndex = topic + "-" + esType + "-" + data.getString(Constants.TIMESTAMP).substring(0, 10)
        data.remove(Constants.TOPIC)
        bulkRequestBuilder.add(client.prepareIndex(esIndex, esType).setSource(data))
      }
      if (bulkRequestBuilder.numberOfActions() > 0) {
        val result = bulkRequestBuilder.get()
        return !result.hasFailures
      }
      true
    } catch {
      case e : Exception =>
        logError("insert es error", e)
        false
    }
  }

  /*
    * 解析失败数据批量插入es
   **/
  def insertBulkError(list : List[JSONObject]): Boolean = {
    try {
      val bulkRequestBuilder: BulkRequestBuilder = client.prepareBulk()
      for (i <- list.indices) {
        val data = list(i)
        val topic = data.getString(Constants.TOPIC)
        val esType = data.getString(Constants.LOG_TYPE)
        val esIndex = topic + "-" + esType + "-" + "error-" + data.getString(Constants.TIMESTAMP).substring(0, 10)
        data.remove(Constants.TOPIC)
        bulkRequestBuilder.add(client.prepareIndex(esIndex, esType).setSource(data))
      }
      if (bulkRequestBuilder.numberOfActions() > 0) {
        val result = bulkRequestBuilder.get()
        return !result.hasFailures
      }
      true
    } catch {
      case e : Exception =>
        logError("insert es error" ,e)
        false
    }
  }
  
    def insertInfluxDBBulk(list : List[java.util.Map[String,String]],esIndexName:String,esTypeName:String) = {
    val bulkRequestBuilder : BulkRequestBuilder = client.prepareBulk()
    for(i <- list.indices){
      bulkRequestBuilder.add(client.prepareIndex(esIndexName,esTypeName).setSource(list(i)))
    }
    if(bulkRequestBuilder.numberOfActions()>0){
      bulkRequestBuilder.get()
    }
  }
}
