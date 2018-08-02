package dealLog.model

import dealLog.util.TimeUtil
import org.apache.commons.lang3.StringUtils

class DealLogMsg extends Serializable {
  var system_name = ""
  var log_type = ""
  var hostname = ""
  var moduleId = ""
  //交易码
  var stdprocode = ""
  //返回码
  var std400mgid = ""
  //流水号
  var stdtermtrc = ""
  //金额
  var stdtranamt = 0d
  var beginTime = ""
  var endTime = ""
  var timestamp = ""
  var msgState = 0

  var money = 0d
  var responseTime = 0l
  var dealStatus = 0

  var logSource = ""

  def gson2ObjIsRight(): Boolean = {
    //判断上文传递的解析Json是否是正确的交易
    if (StringUtils.isNotBlank(system_name) && StringUtils.isNotBlank(stdtermtrc) && StringUtils.isNotBlank(timestamp) && msgState != 0) {
      true
    } else {
      false
    }
  }

  def matchDeal(other: DealLogMsg): DealLogMsg = {
    //更新状态逻辑---1/2(发送/接收)
    if (other.msgState == 1) {
      this.beginTime = other.timestamp
    } else if (other.msgState == 2) {
      this.endTime = other.timestamp
    }
    this.std400mgid = other.std400mgid
    this
  }

  def isCompleteDeal: Boolean = {
    if (StringUtils.isNotBlank(beginTime) && StringUtils.isNotBlank(endTime)) {
      true
    } else {
      false
    }
  }

  def setSuccessProp: Unit = {
    responseTime = TimeUtil.utcToMilltime(endTime) - TimeUtil.utcToMilltime(beginTime)
    if (responseTime < 0) responseTime = 0l
    //dealStatus 0/1 失败/成功
    if (std400mgid == "AAAAAAA") dealStatus = 1
    money = stdtranamt
  }

  def setFailProp(timeOutMill: Long): Unit = {
    responseTime = System.currentTimeMillis() - TimeUtil.utcToMilltime(beginTime)
    dealStatus = 0
    money = stdtranamt
  }

  def getTimestamp: Long = {
    if(StringUtils.isNotBlank(beginTime)){
      TimeUtil.utcToMilltime(beginTime)
    }else{
      TimeUtil.utcToMilltime(timestamp)
    }
  }

  def  getIndetifier:String = {
    system_name + "_" + stdtermtrc
  }
}
