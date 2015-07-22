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
import org.slf4j.LoggerFactory
import com.mongodb.casbah.Imports._


object SendNotificationEmail {

  val receiverList = ("z_h_e_n_h_a_i@mailinator.com " :: getReceivers).mkString("; ")
  val logger = LoggerFactory.getLogger("SendNotificationEmail")

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
    logger.info("DONE")
  }

  
  def getReceivers = {
    val mongoClient = MongoClient("localhost")
    val zhenhaiDB = mongoClient("zhenhai")
    val permissions = 
      zhenhaiDB("permissions").find
        .filter(x => x.get("permissionContent").asInstanceOf[BasicDBList].contains("網站管理－維修行事曆"))
        .map(x => x.get("permissionName").toString)
        .toList ++ List("administrator")
    
    zhenhaiDB("user").filter(x => permissions.contains(x.get("permission"))).map(x => x.get("email")).map(_.toString).toList
  }


  def getNotificationAlarmList: Option[String] = {
    val mongoClient = MongoClient("localhost")
    val zhenhaiDB = mongoClient("zhenhai")
    val alarms = zhenhaiDB("alarm").find(MongoDBObject("isDone" -> false))
    val machineCounterColl = zhenhaiDB("machineCounter")

    def isUrgent(row: DBObject): Boolean = {
      val machineID = row.getAs[String]("machineID").getOrElse("")
      val lastUpdatedCount = row.getAs[Long]("lastReplaceCount").getOrElse(0L)
      val isDone = row.getAs[Boolean]("isDone").getOrElse(false)
      val currentMachineCounter =
        machineCounterColl.findOne(MongoDBObject("machineID" -> machineID))
                          .map(row => row("counter").toString.toLong)
                          .getOrElse(0L)
      val countdownQty = row.getAs[Long]("countdownQty").getOrElse(0L)

      !isDone && (countdownQty + lastUpdatedCount) <= currentMachineCounter
    }

    val alarmNotices = alarms.filter(isUrgent).map { alarm =>
      val machineID = alarm.getAs[String]("machineID").getOrElse("")
      val currentMachineCounter =
        machineCounterColl.findOne(MongoDBObject("machineID" -> machineID))
                          .map(row => row("counter").toString.toLong)
                          .getOrElse(0L)
      val countdownQty = alarm.getAs[Long]("countdownQty").getOrElse(0L)
      val description = alarm.getAs[String]("description").getOrElse("")
      s"[$machineID]  $description ($currentMachineCounter / $countdownQty)"
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
