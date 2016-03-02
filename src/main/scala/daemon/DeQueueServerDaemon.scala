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
import tw.com.zhenhai.model.MachineInfo

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

    /**
     *  取得 RabbitMQ 的 Connection 物件
     *
     *  @return   RabbitMQ 的 Connection 物件
     */
    def getRabbitConnection() = {
      val factory = new ConnectionFactory
      factory.setHost("localhost")
      factory.setUsername("zhenhai")
      factory.setPassword("zhenhai123456")
      factory.newConnection()
    }
  
    /**
     *  取得 RabbitMQ 的 rawData Queue 的連線通道和消化佇列用的 Consumer 物件
     *
     *  rawData queue 是用來存放 RaspberryPi 黑盒子傳入給 CommunicationServer
     *  的原始資料，佇列中的每一個 Message 為 RaspberryPi 傳出的「一行」，也就
     *  是一筆資料。
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

    /**
     *  取得 RabbitMQ 的 productionStatus Queue 的連線通道和消化佇列用的 Consumer 物件
     *
     *  由於計算「訂單狀態」和「今日工單」的部份需要撈資料庫中舊有的資做筆對，會花
     *  相當長的 IO 與計算時間。若將其與其他計算整合在一起，會造成可以快速平行處理
     *  （或循序處理）的部份被 Block 住，而造成明明可以先顯示的資料被卡住。
     *
     *  為了解決這個問題，當我們從 rawData 佇列取出一筆資料後，會將此資料複製一份
     *  到名為 productionStatus 的佇列中，並且額外建立一個 Thread  來處理此佇列中
     *  的資料。
     *
     *  透過這樣的方式，不花時間的資料可以同時處理，而網頁上的「今日工單」和「訂
     *  單狀態」，也只會有數秒至數分鐘的延遲，而不影響使用者的操作。
     *
     *  此函式即會回傳這個 productionStatus 的佇列的 Channel 和 Consumer 物件。
     *
     *  @return     (RabbitMQ 的 Channel 物件, RabbitMQ 的 Consumer 物件)
     */

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

    /**
     *  取得 RabbitMQ 的 operationTime Queue 的連線通道和消化佇列用的 Consumer 物件
     *
     *  由於計算員工的工作時數，以及其他的資料（例如最新機台狀況），必須依照時間順
     *  序計算，不能有 Race condition，否則資料會不準確。
     *
     *  為了解決這個問題，當我們從 rawData 取得一筆資料後，會將資料複制一份到名為
     *  operationTime 的佇列中，並且開另一個 Thread 專門依照佇列中的順序循序處理此
     *  資料。
     *
     *  此函式即會回傳這個 operationTime 的佇列的 Channel 和 Consumer 物件。
     *
     *  @return     (RabbitMQ 的 Channel 物件, RabbitMQ 的 Consumer 物件)
     */
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
     *  循序處理 RabbitMQ 佇列中的資料的 Thread
     *
     *  同上述所說，我們需要另外兩個 Thread 來處理 operationTime 和 productionStatus 這兩
     *  個佇列中的資料，此類別則為兩個 Thread 的共同邏輯。
     *
     *  這個類別會不斷從 channel 與 consumer 中取出資料，並且以 blocking 的方式將資料傳
     *  給 dbProcessor.updateDB() 函式處理，若沒有任何例外發生，則會告知 RabbitMQ 此筆資料
     *  已處理完成，並繼續處理 channel 中的下一筆資料。
     *
     *  @param  dbProcessor     負則處理資料的物件，此類別會呼叫此物件的 updateDB 函式
     *  @param  channel         所使用的 RabbitMQ 的佇列的 Channel
     *  @param  consumer        所使用的 RabbitMQ 的佇列的 Consumer 物件，會從此處取出資料
     */
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

                //通知 RabbitMQ 我們已成功處理此筆資料
                channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)

              } catch {
                case e: Exception => logger.error("Cannot insert to ordered mongoDB", e)
              }
          }
        }
      }

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


    /**
     *  主程式：不斷從佇列中取出其內容，並且分析處理過後存入 MongoDB
     */
    override def run() {
      println(MachineInfo.areaMapping)

      implicit val ec = ExecutionContext.fromExecutor(new java.util.concurrent.ForkJoinPool(12))
  
      // 若發生 Exception，則不斷等待 60 秒後重試
      KeepRetry(60) {
  
        val rabbitConnection = getRabbitConnection
        val (channel, consumer) = initRabbitMQ(rabbitConnection)
        val (productionStatusChannel, productionStatusConsumer) = getProductionStatusQueue(rabbitConnection)
        val (operationTimeChannel, operationTimeConsumer) = getOperationTimeQueue(rabbitConnection)

        val mongoClient = MongoClient("localhost")
   
        logger.info(" [*] DeQueue Server Started.")

        val updateOperationTimeThread = new OrderedDBThread(
          new UpdateOperationTime, 
          operationTimeChannel, 
          operationTimeConsumer
        )

        val updateProductionStatusThread = new OrderedDBThread(
          new UpdateProductionStatus, 
          productionStatusChannel, 
          productionStatusConsumer
        )

        updateOperationTimeThread.start()
        updateProductionStatusThread.start()

        while (!shouldStopped) {
          val delivery = consumer.nextDelivery()
          val message = new String(delivery.getBody())

          val messageHolder = Record(message) orElse Record.processLineWithBug(message)

          messageHolder match {
            case Failure(e) => 
              logger.info(s"Cannot process rawData record $message", e)
              logger.error(s"[BUG2] $message")
              channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)

            case Success(record) =>

              productionStatusChannel.basicPublish("", ProductionStatusQueue, null, message.getBytes());
              operationTimeChannel.basicPublish("", OperationTimeQueue, null, message.getBytes());

              // 以 Non-blocking 的方式處理取出的資料
              val dequeueWork = Future {
                val mongoProcessor = new MongoProcessor(mongoClient)
                processRecord(mongoProcessor, record)

                //通知 RabbitMQ 我們已成功處理此筆資料
                channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
              }
  
              // 若上述的 Future 中發生錯物，則記錄至 Log 檔中
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
