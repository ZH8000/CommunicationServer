package tw.com.zhenhai.model

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat
import java.net.InetAddress
import com.mongodb.casbah.Imports._
import scala.util.Try
import org.slf4j.LoggerFactory

/**
 *  用來表示傳 RaspberryPi 傳回來的記錄資料的物件
 *
 *  @param    rawLotNo              原始工單號（第一個欄位，若是尚未上條碼的機台，固定為 01）
 *  @param    rawPartNo             原始料號（第二個欄位，若是未上條碼的機台，此欄位為寫死的假工單號）
 *  @param    workQty               目標生產量
 *  @param    countQty              良品數
 *  @param    embDate               生產時間 UNIX 戳記，單位為秒
 *  @param    eventQty              非良品的事件數量
 *  @param    machineIP             機台的 IP 位置
 *  @param    eventID               事件代號
 *  @param    machID                機台編號
 *  @param    workID                員工的 MongoDB 的 ID
 *  @param    cxOrStartTimestamp    老化機回傳的 CX 值，或是進維修狀態時相對應的進維修時間
 *  @param    dx                    老化機回傳的 DX 值
 *  @param    lc                    老化機回傳的 LC 值
 *  @param    machineStatus         機台狀態編號
 *  @param    insertDate            此筆資料的原始日期
 *  @param    macAddress            機台的 MAC Address
 *  @param    shiftDate             機台的工班日期
 *  @param    rawData               原始傳回的資料
 */
case class Record(
  rawLotNo: String, 
  rawPartNo: String, 
  workQty: Long, 
  countQty: Long,
  embDate: Long, 
  eventQty: Long,
  machineIP: String,
  eventID: Long,
  machID: String,
  workID: String,
  cxOrStartTimestamp: String,
  dx: String,
  lc: String,
  machineStatus: String,
  insertDate: String,
  macAddress: String,
  shiftDate: String,
  rawData: String
) {

  /**
   *  資料是否是從已實裝條碼機的機台取得
   *
   *  如果是舊的，尚未上條碼的機台，則固定傳回來的第一個欄位是 01，
   *  所以只要這個欄位不是 01，就是有實裝條碼機的機台。
   *
   *  @return     是否為實裝條碼的機台傳回的資料
   */
  lazy val isFromBarcode = rawLotNo.trim != "01"

  /**
   *  工單號碼
   */
  def lotNo = if (!isFromBarcode) rawPartNo else rawLotNo

  /**
   *  料號
   */
  def partNo = if (!isFromBarcode) "none" else rawPartNo

  def area: String = MachineInfo.getMachineArea(this.machID)
  def floor: Int = MachineInfo.getMachineFloor(this.machID)

  /**
   *  取得早班或晚班
   *
   *  @return   若早班則為 M，晚班則為 N
   */
  def shift = {
    val calendar = Calendar.getInstance
    calendar.setTime(new Date(embDate * 1000))
    val hour = calendar.get(Calendar.HOUR_OF_DAY)
    val minute = calendar.get(Calendar.MINUTE)
    val hostname = InetAddress.getLocalHost().getHostName()

    val isDailyShift = hostname match {
      case "ZhenhaiServerSZ" => (hour >= 7 && minute >= 30) && (hour <= 19 && minute < 30)
      case _ => (hour >= 7 && hour < 19)
    }

    if (isDailyShift) "M" else "N"
  }

  /**
   *  φ 別
   *
   *  若條碼中有則從條碼取得，否則從機台與φ別對照表取得
   */
  def product = isFromBarcode match {
    case true  => getProductFromBarcode.getOrElse("Unknown")
    case false => MachineInfo.getProduct(machID)
  }

  /**
   *  從料號中取得 φ 別
   *
   *  φ 別位於料號的第 11 至 14 碼，前兩碼為直徑，後兩碼為高度
   */
  def getProductFromBarcode: Try[String] = Try {
    val radius = partNo.substring(10,12).toInt
    val height = partNo.substring(12,14).toInt
    radius + "x" + height
  }


  /*
   *  料號中完整的產品尺吋代碼
   */
  def fullProductCode = isFromBarcode match {
    case true   => Try{partNo.substring(10, 15)}.getOrElse("Unknown")
    case false  => "Unknown"
  }

  /**
   *  統一錯誤事件 ID
   */
  val defactID = {

    val defactIDOption = for {
      machineModel      <- MachineInfo.machineModel.get(machID)
      eventIDToDefactID <- MachineInfo.defactEventTable.get(machineModel)
      defactID          <- eventIDToDefactID.get(eventID.toInt)
    } yield defactID

    defactIDOption.getOrElse(-1)
  }

  /**
   *  統一統計事件 ID
   */
  val otherEventID = {

    val otherEventIDOption = for {
      machineModel            <- MachineInfo.machineModel.get(machID)
      eventIDToOtherEventID   <- MachineInfo.otherEventTable.get(machineModel)
      otherEventID            <- eventIDToOtherEventID.get(eventID.toInt)
    } yield otherEventID

    otherEventIDOption.getOrElse(-1)
  }

  /**
   *  φ 的別直徑，若無法正確分析取得則為 -1
   */
  def capacityPrefix = {
    product.split("x") match {
      case Array("Unknown") => -1
      case Array(first, second) => first.toDouble
      case Array(first) => first.toDouble
      case _ => -1
    }
  }

  /**
   *  取得φ別的範圍（大中小φ）
   *
   *  共有：
   *
   *   - Unknown （無法分析）
   *   - 5 - 8 （小）
   *   - 10 - 12.5（中）
   *   - 16 - 18（大）
   */
  def capacityRange = capacityPrefix match {
    case -1                         => "Unknown"
    case x if x >= 5 && x <= 8      => "5 - 8"
    case x if x >=10 && x <= 12.5   => "10 - 12.5"
    case x if x >=16 && x <= 18     => "16 - 18"
    case x                          => x.toString
  }

  /**
   *  取得機台製程料型 ID
   */
  def machineType: Int = MachineInfo.getMachineTypeID(this.machID)

  /**
   *  取得料號中的顧客代碼
   */
  lazy val customer = Try { partNo.substring(19, 23) }.getOrElse("Unknown")

  /**
   *  取得精細度至十分鐘的人工可讀時間戳記
   */
  lazy val tenMinute = {
    val dateTimeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")
    dateTimeFormatter.format(embDate * 1000).substring(0, 15) + "0"
  }

  /**
   *  將此記錄物件轉換成 MongoDB 的資料列，以方便存入 MongoDB
   */
  def toMongoObject = MongoDBObject(
    "insertDate" ->  insertDate,
    "shiftDate" -> shiftDate,
    "machineID" -> machID,
    "rawData" -> rawData
  )

}

/**
 *  用來將由 RaspberryPi 傳來的原始資料轉換成 Record 物件
 */
object Record {

  implicit val logger = LoggerFactory.getLogger("DeQueueServer")

  /**
   *  將原始的時間戳記依照謝崗廠的換幫時間轉換成工班日期（減七個小時）
   *
   *  @param    timestamp   時間戳記
   *  @return               工班日期的時間戳記
   */
  def getShiftTimeOfXG(timestamp: Long) = {
    val offsetOf7Hours = 7 * 60 * 60 * 1000
    new Date((timestamp * 1000) - offsetOf7Hours)    
  }

  /**
   *  將原始的時間戳記依照蘇州廠的換幫時間轉換成工班日期（減七個半小時）
   *
   *  @param    timestamp   時間戳記
   *  @return               工班日期的時間戳記
   */
  def getShiftTimeOfSZ(timestamp: Long) = {
    val offsetOf7Hours = (7 * 60 * 60 * 1000) + (30 * 60 * 1000)
    new Date((timestamp * 1000) - offsetOf7Hours)
  }


  /**
   *  將原始的時間戳記轉換成工班日期
   *
   *  謝崗廠：減七個小時（早上七點上班，上到晚上六點五十九）
   *  蘇州廠：減七個半小時（早上七點半上班，上到晚上七點二十九）
   *
   *  @param    timestamp   時間戳記
   *  @return               工班日期的時間戳記
   */
  def getShiftTime(timestamp: Long) = {
    val hostname = InetAddress.getLocalHost().getHostName()

    hostname match {
      case "ZhenhaiServerSZ" => getShiftTimeOfSZ(timestamp)
      case _ => getShiftTimeOfXG(timestamp)
    }
  }


  def processLineWithBug(line: String) = Try {

    logger.info(s"[BUG LINE] $line")
    val columns = line.split(" ");
    val machineID = columns(9)
    val timestamp = columns(5).toLong
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    new Record(
      columns(0), 
      columns(1) + " " + columns(2), 
      columns(3).toLong, 
      columns(4).toLong,
      columns(5).toLong,
      columns(6).toLong, 
      columns(7),
      columns(8).toLong,
      machineID,
      columns(10).toLowerCase,
      columns(11),
      columns(12),
      columns(13),
      columns(14).trim,
      dateFormatter.format(new Date(timestamp * 1000)),
      Try{columns(15)}.getOrElse(""),
      dateFormatter.format(getShiftTime(timestamp)),
      line
    )

  }

  /**
   *  用來將由 RaspberryPi 傳來的原始資料轉換成 Record 物件
   *
   *  @param    line        從 RaspberryPi 傳來的原始資料
   *  @return               轉換過後的 Record 物件
   */
  def apply(line: String) = Try {
    val columns = line.split(" ");
    val machineID = columns(8)
    val timestamp = columns(4).toLong
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

    new Record(
      columns(0), 
      columns(1), 
      columns(2).toLong, 
      columns(3).toLong,
      columns(4).toLong,
      columns(5).toLong, 
      columns(6),
      columns(7).toLong,
      machineID,
      columns(9).toLowerCase.take(24),
      columns(10),
      columns(11),
      columns(12),
      columns(13).trim,
      dateFormatter.format(new Date(timestamp * 1000)),
      Try{columns(14)}.getOrElse(""),
      dateFormatter.format(getShiftTime(timestamp)),
      line
    )
  }
}
