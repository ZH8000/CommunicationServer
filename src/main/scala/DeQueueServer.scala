package tw.com.zhenhai

import scala.concurrent._
import ExecutionContext.Implicits.global

import tw.com.zhenhai.db.MongoProcessor
import tw.com.zhenhai.model.Record
import tw.com.zhenhai.util.KeepRetry

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel
import com.rabbitmq.client.QueueingConsumer

import org.slf4j.LoggerFactory
import com.mongodb.casbah.Imports._


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
 
      logger.info(" [*] DeQueue Server Started.")

      while (!shouldStopped) {
        val delivery = consumer.nextDelivery()
        val message = new String(delivery.getBody())

        val dequeueWork = Future {
          val mongoProcessor = new MongoProcessor(mongoClient)
 
          Record(message).foreach{ record =>

            record.countQty match {
              case -1 => mongoProcessor.addMachineAlert(record)
              case  n => mongoProcessor.addRecord(record)
            }

            if (record.countQty == 0 && record.badQty == 0) {
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



