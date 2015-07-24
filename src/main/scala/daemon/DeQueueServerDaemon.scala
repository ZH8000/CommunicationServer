package tw.com.zhenhai.daemon

import com.mongodb.casbah.Imports._
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.QueueingConsumer

import java.io._
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.daemon.Daemon
import org.apache.commons.daemon.DaemonContext
import org.apache.commons.daemon.DaemonInitException

import org.slf4j.LoggerFactory

import tw.com.zhenhai._
import tw.com.zhenhai.db.MongoProcessor
import tw.com.zhenhai.model.Record
import tw.com.zhenhai.util.KeepRetry

import scala.concurrent._
import ExecutionContext.Implicits.global

class DeQueueServerDaemon extends Daemon {

  var serverThread = new DeQueueServerThread

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


class DeQueueServerThread extends Thread {

  implicit val logger = LoggerFactory.getLogger("DeQueueServer")

  var shouldStopped = false
  val QueueName = "rawDataLine"

  def initRabbitMQ() = {
     val factory = new ConnectionFactory
     factory.setHost("localhost")
     factory.setUsername("zhenhai")
     factory.setPassword("zhenhai123456")
     val connection = factory.newConnection()
     val channel = connection.createChannel()
     channel.queueDeclare(QueueName, true, false, false, null)
     channel.basicQos(10)

     val consumer = new QueueingConsumer(channel)
     channel.basicConsume(QueueName, false, consumer)
     (channel, consumer)
  }

  override def run() {

    KeepRetry(60) {

      val (channel, consumer) = initRabbitMQ()
      var recordCount: Long = 0
      val mongoClient = MongoClient("localhost")
      val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
 
      logger.info(" [*] DeQueue Server Started.")

      while (!shouldStopped) {
        val delivery = consumer.nextDelivery()
        val message = new String(delivery.getBody())

        val dequeueWork = Future {
          val mongoProcessor = new MongoProcessor(mongoClient)
          val zhenhaiRawDB = mongoClient("zhenhaiRaw")
          val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
          val rawTable = zhenhaiRawDB(dateFormatter.format(new Date))

          rawTable.insert(MongoDBObject("data" -> message))

          Record(message).foreach{ record =>

            record.countQty match {
              case -1 => mongoProcessor.addMachineAlert(record)
              case  n => mongoProcessor.addRecord(record)
            }

            if (record.countQty == 0 && record.eventQty == 0) {
              logger.info(s" [!] [strange] DeQueue: $message")
            }
          }

          channel.basicAck(delivery.getEnvelope.getDeliveryTag, false)
        }

        dequeueWork.onFailure { 
          case e: Exception => logger.error("Cannot insert to mongoDB", e)
        }
      }

      logger.info(" [*] DeQueue Server Stopped.")
    }
  }

}



