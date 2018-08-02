package dealLog.resource

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryUntilElapsed

object ResourceConnPool {
  private val zkServers: String = "10.20.32.65:2181"
  var curatorFramework: CuratorFramework= _

  def getZkConn(): CuratorFramework = {
    if(curatorFramework == null){
      curatorFramework = CuratorFrameworkFactory
        .builder
        .connectString(zkServers)
        .connectionTimeoutMs(1000)
        .sessionTimeoutMs(10000)
        .retryPolicy(new RetryUntilElapsed(1000, 1000)).build

      curatorFramework.start
      try
        Thread.sleep(1500)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }

    curatorFramework
  }

}
