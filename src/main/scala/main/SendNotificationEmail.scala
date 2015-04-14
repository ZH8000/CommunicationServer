package tw.com.zhenhai.main

import java.util.Properties
import javax.mail.PasswordAuthentication
import javax.mail.Authenticator
import javax.mail.Session
import javax.mail.internet.InternetAddress
import javax.mail.internet.MimeMessage
import javax.mail.Message
import javax.mail.Transport
import javax.mail.Address
import com.mongodb.casbah.Imports._
<<<<<<< HEAD
import org.slf4j.LoggerFactory
import com.mongodb.casbah.Imports._


=======
>>>>>>> baf6c88075f37b6afd3d15ba2837c1ceea03b059

object SendNotificationEmail {

  val receiverList = "brianhsu.hsu@gmail.com"
<<<<<<< HEAD
  val logger = LoggerFactory.getLogger("SendNotificationEmail")
=======
>>>>>>> baf6c88075f37b6afd3d15ba2837c1ceea03b059

  def sendmail(username: String, password: String, body: String) {
    val props = new Properties
    props.put("mail.smtp.auth", "true");
    props.put("mail.smtp.host", "mail.zhenhai.com.tw");
    props.put("mail.smtp.port", "25");

    val authenticator = new Authenticator() {
      override def getPasswordAuthentication(): PasswordAuthentication = new PasswordAuthentication(username, password)
    }

    val session = Session.getInstance(props, authenticator)
    val message = new MimeMessage(session)
    val receivers: Array[Address] = InternetAddress.parse(receiverList).asInstanceOf[Array[Address]]

    message.setFrom(new InternetAddress(username))
    message.setRecipients(Message.RecipientType.TO, receivers)
    message.setSubject("今日維修零件清單")
    message.setText(body)
    Transport.send(message);
<<<<<<< HEAD
    logger.info("DONE")
=======
    println("DONE")
>>>>>>> baf6c88075f37b6afd3d15ba2837c1ceea03b059
  }

  def getNotificationAlarmList: Option[String] = {
    val mongoClient = MongoClient("localhost")
    val zhenhaiDB = mongoClient("zhenhai")
    val alarms = zhenhaiDB("alarm").find(MongoDBObject("isDone" -> false))

    def isUrgent(alarm: DBObject) = alarm.get("countQty").toString.toInt >= alarm.get("countdownQty").toString.toInt
    val alarmNotices = alarms.filter(isUrgent).map { alarm =>
      val countQty = alarm.get("countQty").toString.toInt
      val countdownQty = alarm.get("countdownQty").toString.toInt
      val machineID = alarm.get("machineID").toString
      val description = alarm.get("description").toString
      s"[$machineID]  $description ($countQty / $countdownQty)"
    }

    alarmNotices.isEmpty match {
      case true => None
      case false =>
        Some(
          """
          | 以下為今日需要進行定期零件更換的機台：
          |
          |
          |  [機台編號]  描述  (目前良品數 / 目標良品數)
          |
          |  %s
          """.stripMargin.format(alarmNotices.mkString("\n"))
        )
    }

  }

  def main(args: Array[String]) {

    logger.info("Send notification......" + new java.util.Date)

    getNotificationAlarmList.foreach { alarmNotices => 
      val username = args(0).trim
      val password = args(1).trim

      logger.info("notices:" + alarmNotices)
      sendmail(username, password, alarmNotices)
    }
  }
}
