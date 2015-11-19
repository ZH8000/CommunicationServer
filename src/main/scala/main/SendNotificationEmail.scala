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

/**
 *  發信通知零件更換
 *
 *  這隻程式會從 MongoDB 資料庫中，取得零件更換行事曆當中，需要更換的記錄，
 *  並發信通知
 */
object SendNotificationEmail {

  val receiverList = ("z_h_e_n_h_a_i@mailinator.com" :: getReceivers).mkString(",")
  val logger = LoggerFactory.getLogger("SendNotificationEmail")

  /**
   *  透過振每的 SMTP Server 送信
   *
   *  @param    username  使用者名稱
   *  @param    password  密碼
   *  @param    body      信件內容
   */
  def sendmail(username: String, password: String, title: String, body: String) {
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
    message.setSubject(title)
    message.setText(body)
    Transport.send(message);
    logger.info("DONE")
  }

  
  /**
   *  取得收信人列表
   *
   *  先查詢 MongoDB 中有哪些的 permissions 有「網站管理－維修行事曆」這個權限，
   *  並且過濾出具有此權限的使用者。
   *
   *  @return     具有「網站管理－維修行事曆」的使用者的電子郵件位址
   *
   */
  def getReceivers: List[String] = {
    val mongoClient = MongoClient("localhost")
    val zhenhaiDB = mongoClient("zhenhai")

    // 取得
    val permissions = 
      zhenhaiDB("permissions").find
        .filter(x => x.get("permissionContent").asInstanceOf[BasicDBList].contains("網站管理－維修行事曆"))
        .map(x => x.get("permissionName").toString)
        .toList ++ List("administrator")
   
    zhenhaiDB("user").filter(x => permissions.contains(x.get("permission"))).map(x => x.get("email")).map(_.toString).toList
  }


  /**
   *  取得通知信件內容
   *
   *  此函式會檢查 MongoDB 中的 alarm 資料表，且看是否有零件已經經過設定的
   *  更換循環但仍未更換，若有，則建立通知信件的內容。
   *
   *  @return     若有需要更換的零件，則回傳 Some(信件內容)，否則為 None
   */
  def getNotificationAlarmEmail: Option[String] = {
    val mongoClient = MongoClient("localhost")
    val zhenhaiDB = mongoClient("zhenhai")
    val alarms = zhenhaiDB("alarm").find(MongoDBObject("isDone" -> false))
    val machineCounterColl = zhenhaiDB("machineCounter")

    /**
     *  是否為需要寄發通知零件更換設定
     *
     *  @param      row     從 alarm 資料表取出的資料列
     *  @return             若需要更換則為 true，否則為 false
     */
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

    getNotificationAlarmEmail.foreach { alarmNotices => 
      val username = args(0).trim
      val password = args(1).trim

      logger.info("notices:" + alarmNotices)
      sendmail(username, password, "今日維修零件清單", alarmNotices)
    }
  }
}
