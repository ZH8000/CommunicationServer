package tw.com.zhenhai.daemon

import tw.com.zhenhai._

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import com.rabbitmq.client.{Connection => RabbitMQConnection, Channel => RabbitMQChannel}
import java.net.{ServerSocket, Socket}
import org.apache.commons.daemon.Daemon
import org.apache.commons.daemon.DaemonContext
import org.apache.commons.daemon.DaemonInitException
import org.slf4j.LoggerFactory
import resource._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.io._
import tw.com.zhenhai.db.MongoProcessor
import tw.com.zhenhai.model.Record
import tw.com.zhenhai.util.KeepRetry

/**
 *  將 RaspberryPi 的資料寫入 RabbitMQ 當中
 *
 *  在舊的系統設計上，RaspberryPi 會將資料送到 EnQueueServer，再由 EnQueueServer 將
 *  資料寫入到 RabbitMQ 中，最後再用 DeQueue Server 從 RabbitMQ 中取出，如下圖所示：
 *
 *               送　　　　　　　　　送　　　　　　　　拉
 *  RaspberryPi ---＞  EnQueueServer --＞ RabbitMQ  ＜---  DeQueueServer
 *
 *  但由於這樣會反而多了一層 EnQueueServer，容易造成不穩定，因此新的版本改成：
 *
 *
 *              送　　　　送　　　　　　　　拉
 *  RaspberryPi ---＞  RabbitMQ  ＜---  DeQueueServer
 *
 *  理論上此處的程式碼不應該再被使用，但為防止有些 RaspberryPi 的程式尚是舊版的，所以
 *  暫時保留，待確認 RaspberryPi 全數更新後，才做移除。
 *
 */
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

  class EnQueueServerThread extends Thread {
  
    implicit val logger = LoggerFactory.getLogger("EnQueueServer")
  
    var shouldStopped = false
    val QueueName = "rawDataLine"
  
    def initRabbitConnection() = {
      val factory = new ConnectionFactory
      factory.setUsername("zhenhai")
      factory.setPassword("zhenhai123456")
  
      factory.newConnection()
    }
  
    def initRabbitChannel(connection: RabbitMQConnection) = {
      val channel = connection.createChannel()
      channel.queueDeclare(QueueName, true, false, false, null)
      channel
    }
  
    def processInput(socket: Socket, channel: RabbitMQChannel, counter: Long) {
      for {
        managedSocket <- managed(socket)
        inputStream <- managed(socket.getInputStream)
        bufferedSource <- managed(new BufferedSource(inputStream))
      } {
        val line = bufferedSource.getLines().next()
  
        logger.info(s" [*] [$counter] EnQueue: $line")
  
        if (line != "saveData") {
          channel.basicPublish(
            "", QueueName, MessageProperties.PERSISTENT_TEXT_PLAIN, 
            line.getBytes
          )
        }
      }
    }
  
    override def start() {
      this.shouldStopped = false
      super.start()
    }
  
    override def run() {
  
  
      KeepRetry(5) {
  
        var counter = 0L
  
        for {
          server <- managed(new ServerSocket(5566))
          rabbitConnection <- managed(initRabbitConnection())
          channel <- managed(initRabbitChannel(rabbitConnection))
        } {
  
          logger.info(" [*] EeQueue Server Started.")
  
          while (!shouldStopped) {
            val socket = server.accept()
            Future {
              counter += 1
              processInput(socket, channel, counter)
            }
          }
  
          logger.info(" [*] EeQueue Server Stopped.")
        }
      }
  
    }
  
  }

}




