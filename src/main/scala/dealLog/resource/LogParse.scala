package dealLog.resource

import com.alibaba.fastjson.{JSON, JSONObject}
import dealLog.model.Constants
import dealLog.util.{TimeUtil, XMLLogParse}
import org.apache.spark.internal.Logging

object LogParse extends Logging{
  def parse(data : String) : JSONObject = {
    try {
      val dataObject = JSON.parseObject(data)
      val message = dataObject.getString(Constants.MESSAGE)

      val xmlStart = message.indexOf("<")
      val xmlEnd = message.lastIndexOf(">")
      val json = XMLLogParse.xmlParse(message.substring(xmlStart, xmlEnd + 1))
      val messageObject = JSON.parseObject(json)
      //dataObject.putAll(messageObject)

      val logType = dataObject.getString(Constants.LOG_TYPE)
      var time : String = null


      if(data.contains("SEND") || data.contains("send") || data.contains("Send")){
        val timeStart = data.indexOf("TIME")
        val timeEnd = data.indexOf("]" ,timeStart)
        time = TimeUtil.timeStampFormat(data.substring(timeStart+5,timeEnd), "yyyy/MM/dd HH:mm:ss.SSS")
        //jsonObject.put(Constants.MSG_STATE,2)
      }else if(data.contains("RECV") || data.contains("recv") || data.contains("Recv")){
        val timeStart = data.indexOf(":")
        val timeEnd = data.indexOf("|")
        time = TimeUtil.timeStampFormat(data.substring(timeStart + 1, timeEnd),"yyyy-MM-dd HH:mm:ss.SSS")
        //jsonObject.put(Constants.MSG_STATE, 1)
      }
      //jsonObject.put(Constants.TIMESTAMP, time)
      //jsonObject.put(Constants.SYSTEM_NAME, "test")
      //jsonObject.put(Constants.LOG_TYPE, "test")
      //jsonObject.put(Constants.HOST, "10.10.10.10")
      //jsonObject.put(Constants.PARSE_STATUS, 1)//1-解析成功，0-解析失败
      dataObject
    } catch {
      case e : Exception =>
        logWarning("log parse error" ,e)
        val jsonObject = new JSONObject()
        jsonObject.put(Constants.SOURCE, data)
        jsonObject.put(Constants.PARSE_STATUS,0)
        jsonObject
    }
  }
}
