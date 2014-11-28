package tw.com.zhenhai.main

import tw.com.zhenhai._

object DeQueueServer {
  def main(args: Array[String]) = {

    val serverThread = new DeQueueServerThread
    serverThread.start()
    serverThread.join()
  }
}

