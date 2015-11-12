package tw.com.zhenhai.daemon

import com.mongodb.casbah.Imports._
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.QueueingConsumer
import com.rabbitmq.client.MessageProperties

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.daemon.Daemon
import org.apache.commons.daemon.DaemonContext
import org.apache.commons.daemon.DaemonInitException

import org.slf4j.LoggerFactory

import tw.com.zhenhai._
import tw.com.zhenhai.db._
import tw.com.zhenhai.model.Record
import tw.com.zhenhai.util.KeepRetry

import scala.util._
import scala.concurrent._

/**
 *  將 RabbitMQ 中的資料拿出來處理的 Daemon
 *
 *  此類別將「把資料拿出來分析處理後存到 MongoDB」的程式，包裝
 *  成 JSVC 的 Daemon，以方便在 Linux 系統上使用。
 *
 */
class DeQueueServerDaemon extends Daemon {

  var serverThread = new DeQueueServerThread

  val ProductionStatusQueue = "productionStatusQueue"
  val OperationTimeQueue = "operationTimeQueue"
  val RawDataQueue = "rawDataLine"

  /*================================================
   *  JSVC Daemon 標準 API 界面實作
   *==============================================*/
  override def start() {
    serverThread.start()
  }

  override def stop() {
    serverThread.shouldStopped = true
    serverThread.join(2000)
  }

  override def init(context: DaemonContext) { }
  override def destroy() { }


  /**
   *  此 Thread 用來不斷從 RabbitMQ 中撈資料，並寫到 MongoDB 當中
   */
  class DeQueueServerThread extends Thread {
  
    implicit val logger = LoggerFactory.getLogger("DeQueueServer")
  
    var shouldStopped = false

    def getRabbitConnection() = {
      val factory = new ConnectionFactory
      factory.setHost("localhost")
      factory.setUsername("zhenhai")
      factory.setPassword("zhenhai123456")
      factory.newConnection()
    }
  
    /**
     *  初始化 RabbitMQ 並取得 RabbitMQ 的連線通道和消化佇列用的 Consumer 物件
     *
     *  @return     (RabbitMQ 的 Channel 物件, RabbitMQ 的 Consumer 物件)
     */
    def initRabbitMQ(connection: Connection) = {

      val channel = connection.createChannel()
      channel.queueDeclare(
        RawDataQueue, 
        true,      // durable - will the queue survive a server restart?
        false,     // exclusive - is restricted to this connection?
        false,     // autoDelete - will server delete it when no longer in use?
        null       // arguments, not in use
      )

      // 一次最多從佇列中取出十個還沒處理的訊息
      channel.basicQos(10)
 
      val consumer = new QueueingConsumer(channel)
      channel.basicConsume(RawDataQueue, false, consumer)
      (channel, consumer)
    }

    def getProductionStatusQueue(connection: Connection) = {
      val channel = connection.createChannel()
      channel.queueDeclare(
        ProductionStatusQueue, 
        true,      // durable - will the queue survive a server restart?
        false,     // exclusive - is restricted to this connection?
        false,     // autoDelete - will server delete it when no longer in use?
        null       // arguments, not in use
      )

      // 一次最多從佇列中取出十個還沒處理的訊息
      channel.basicQos(5)
 
      val consumer = new QueueingConsumer(channel)
      channel.basicConsume(ProductionStatusQueue, false, consumer)
      (channel, consumer)
    }

    def getOperationTimeQueue(connection: Connection) = {
      val channel = connection.createChannel()
      channel.queueDeclare(
        OperationTimeQueue, 
        true,      // durable - will the queue survive a server restart?
        false,     // exclusive - is restricted to this connection?
        false,     // autoDelete - will server delete it when no longer in use?
        null       // arguments, not in use
      )

      // 一次最多從佇列中取出十個還沒處理的訊息
      channel.basicQos(5)
 
      val consumer = new QueueingConsumer(channel)
      channel.basicConsume(OperationTimeQueue, false, consumer)
      (channel, consumer)
    }

    
    /**
     *  處理從佇列中取得的資料
     *
     *  @param    mongoProcessor    負責處理統計資訊並寫入 MongoDB 的物件
     *  @param    record            已經轉換過的，從 RaspberryPi 傳來的資料的物件
     */
    def processRecord(mongoProcessor: MongoProcessor, record: Record) {

      // 當 RaspberryPi 判斷其接受到的生產機台訊號有誤時，會將 countQty 欄位設成 -1，
      // 此時不應將其納入統計資料，而是加到獨立的資料表以供除錯。
      record.countQty match {
        case -1 => mongoProcessor.addMachineAlert(record)
        case  n => mongoProcessor.addRecord(record)
      }
  
      if (record.countQty == 0 && record.eventQty == 0) {
        logger.info(s" [!] [strange] DeQueue: ${record.rawData}")
      }
    }

    class OrderedDBThread(dbProcessor: OrderedDBProcessor, channel: Channel, consumer: QueueingConsumer) extends Thread {

      override def run() {
        logger.info(s"Start orderedDBThread of ${dbProcessor.getClass}")
        while (!shouldStopped) {
          val delivery = consumer.nextDelivery()
          val message = new String(delivery.getBody())
          val messageHolder = Record(message) orElse Record.processLineWithBug(message)

          messageHolder match {
            case Failure(e) => logger.error(s"Cannot process rawData record $message", e)
            case Success(record) =>
              try {
                dbProcessor.updateDB(record)
                channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
              } catch {
                case e: Exception => logger.error("Cannot insert to ordered mongoDB", e)
              }
          }
        }
      }

    }

    /**
     *  主程式：不斷從佇列中取出其內容，並且分析處理過後存入 MongoDB
     */
    override def run() {

      implicit val ec = ExecutionContext.fromExecutor(new java.util.concurrent.ForkJoinPool(12))
  
      // 若發生 Exception，則不斷等待 60 秒後重試
      KeepRetry(60) {
  
        val rabbitConnection = getRabbitConnection
        val (channel, consumer) = initRabbitMQ(rabbitConnection)
        val (productionStatusChannel, productionStatusConsumer) = getProductionStatusQueue(rabbitConnection)
        val (operationTimeChannel, operationTimeConsumer) = getOperationTimeQueue(rabbitConnection)

        var recordCount: Long = 0
        val mongoClient = MongoClient("localhost")
   
        logger.info(" [*] DeQueue Server Started.")

        logger.info(" [*] Try to start thread...")
        val updateOperationTimeThread = new OrderedDBThread(new UpdateOperationTime, operationTimeChannel, operationTimeConsumer)
        val updateProductionStatusThread = new OrderedDBThread(new UpdateProductionStatus, productionStatusChannel, productionStatusConsumer)

        logger.info(" [*] Try to stat updateOperationTimeThread...")

        updateOperationTimeThread.start()

        logger.info(" [*] Try to stat updateProductionStatusThread...")
        updateProductionStatusThread.start()

        while (!shouldStopped) {
          val delivery = consumer.nextDelivery()
          val message = new String(delivery.getBody())

          val messageHolder = Record(message) orElse Record.processLineWithBug(message)

          messageHolder match {
            case Failure(e) => logger.info(s"Cannot process rawData record $message", e)
            case Success(record) =>

              productionStatusChannel.basicPublish("", ProductionStatusQueue, null, message.getBytes());
              operationTimeChannel.basicPublish("", OperationTimeQueue, null, message.getBytes());

              val dequeueWork = Future {
                val mongoProcessor = new MongoProcessor(mongoClient)
                processRecord(mongoProcessor, record)
                //通知 RabbitMQ 我們已成功處理此筆資料
                channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
              }
  
              dequeueWork.onFailure { 
                case e: Exception => logger.error("Cannot insert to mongoDB", e)
              }
          }
        }

        logger.info("MainThread stoped....")
        updateOperationTimeThread.join()
        updateProductionStatusThread.join()
 
        logger.info(" [*] DeQueue Server Stopped.")
      }
    }
  }
}
