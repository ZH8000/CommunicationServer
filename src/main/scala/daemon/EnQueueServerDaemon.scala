package tw.com.zhenhai.daemon

import tw.com.zhenhai._

import org.apache.commons.daemon.Daemon
import org.apache.commons.daemon.DaemonContext
import org.apache.commons.daemon.DaemonInitException

class EnQueueServerDaemon extends Daemon {

  var serverThread = new EnQueueServerThread

  override def start() {
    serverThread.start()
  }

  override def stop() {
    serverThread.shouldStopped = true
    serverThread.join(2000)
  }

  override def init(context: DaemonContext) { }
  override def destroy() { }

}

