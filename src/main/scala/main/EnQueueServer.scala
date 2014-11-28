package tw.com.zhenhai.main

import tw.com.zhenhai._

object EnQueueServer {
  def main(args: Array[String]) = {
    val serverThread = new EnQueueServerThread
    serverThread.start()
    serverThread.join()
  }
}
