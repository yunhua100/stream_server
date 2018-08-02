package dealLog.resource

import dealLog.model.DealLogMsg
import org.influxdb.InfluxDB
import org.influxdb.dto.Point

object InfluxDBTemplate {
  def insertData(influxDB: InfluxDB, measurement: String, msg: DealLogMsg): Unit = {
    influxDB.write(Point.measurement(measurement)
      .time(msg.getTimestamp, java.util.concurrent.TimeUnit.MILLISECONDS)
      .tag("system_name", msg.system_name)
      .tag("log_type", msg.log_type)
      .tag("host", msg.hostname)
      .tag("moduleId", msg.moduleId)
      .tag("tranId", msg.stdprocode)
      .tag("std400mgid", msg.std400mgid)
      .addField("money", msg.money)
      .addField("responseTime", msg.responseTime)
      .addField("dealStatus", msg.dealStatus)
      .build())
  }
}
