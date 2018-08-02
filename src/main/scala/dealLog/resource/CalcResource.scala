package dealLog.resource
import com.google.gson.Gson
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryUntilElapsed
import org.influxdb.BatchOptions
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory

object CalcResource {
  lazy val gson = new Gson()
  private var zkClient:CuratorFramework = _
  private var influxDB:InfluxDB = _

  def getZkClient(zkServers: String): CuratorFramework = {
    if (zkClient == null) {
      zkClient = CuratorFrameworkFactory
        .builder.connectString(zkServers)
        .connectionTimeoutMs(1000)
        .sessionTimeoutMs(10000)
        .retryPolicy(new RetryUntilElapsed(1000, 1000))
        .build

      Thread.sleep(1000)
    }
    zkClient
  }

  def getInfluxDB(influxDBUrl: String, dbName: String): InfluxDB = {
    if (influxDB == null) {
      influxDB = InfluxDBFactory
        .connect(influxDBUrl)
        .setDatabase(dbName)
        .enableBatch(BatchOptions.DEFAULTS)
    }

    influxDB
  }
}
