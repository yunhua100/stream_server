import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.net.InetAddress
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import dealLog.model.Constants
import dealLog.resource.{ConfLoad, LogParse, Parse}
import dealLog.util.{ElasticClient, TimeUtil, XMLLogParse}
import net.sf.json.xml.XMLSerializer
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.common.transport.TransportAddress

import scala.util.control.Breaks._
import scala.io.Source


object Test {
  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("E:\\test\\20180622.log","UTF-8")
    val lines = file.getLines()
    var correctResult = List[JSONObject]()
    breakable{
      for(line <- lines){
        println(line)
        var i = 0
        /*while(i<100){
          val result = Parse.parse(line)
          i = i+1
          result.put(Constants.TOPIC,"tr-nebs")
          result.put(Constants.TIMESTAMP, TimeUtil.test(System.currentTimeMillis()+i*1000000))
          correctResult = correctResult :+ result
        }*/
        val result = Parse.parse(line)
        println(result)
        break()
      }
    }
    //ElasticClient.getInstance().insertBulkCorrect(correctResult)
    file.close()



    /*val jo : JSONObject = new JSONObject()
    jo.put("key0000","value0000")
    val list : List[JSONObject] = List(jo)
    ElasticClient.getInstance().insertBulk(list)*/

   /* val jSONObject = new JSONObject()
    jSONObject.put("key1","value1")
    jSONObject.put("key2","value2")
    if(jSONObject.containsKey("key"))
      jSONObject.remove("ke")
    println(jSONObject)*/


  }

  def parse(line : String) : String = {
    val xmlSerializer = new XMLSerializer
    val json = xmlSerializer.read(line)
    json.toString()
  }


}
