package tw.com.zhenhai.model

import java.util.Date
import java.text.SimpleDateFormat
import com.mongodb.casbah.Imports._
import scala.util.Try

case class Record(
  orderType: String, 
  lotNo: String, 
  workQty: Long, 
  countQty: Long,
  embDate: Long, 
  badQty: Long,
  machineIP: String,
  defactID: Long,
  machID: String,
  workID: String,
  cx: String,
  dx: String,
  lc: String,
  machineStatus: String,
  product: String,
  insertDate: String,
  macAddress: String,
  shiftDate: String
) {
  def toMongoObject = MongoDBObject(
    "order_type" -> orderType,
    "lot_no" -> lotNo,
    "work_qty" -> workQty,
    "count_qty" -> countQty, 
    "emb_date" -> embDate,
    "bad_qty" -> badQty,
    "mach_ip" -> machineIP,
    "defact_id" -> defactID,
    "mach_id" -> machID,
    "work_id" -> workID,
    "CX" -> cx,
    "DX" -> dx,
    "LC" -> lc,
    "mach_status" -> machineStatus,
    "product" -> product,
    "insertDate" ->  insertDate,
    "mac_address" -> macAddress,
    "shiftDate" -> shiftDate
  )
}

object Record {

  def apply(dbObject: DBObject) = new Record(
    dbObject("order_type").toString,
    dbObject("lot_no").toString,
    dbObject("work_qty").toString.toLong,
    dbObject("count_qty").toString.toLong,
    dbObject("emb_date").toString.toLong,
    dbObject("bad_qty").toString.toLong,
    dbObject("mach_ip").toString,
    dbObject("defact_id").toString.toLong,
    dbObject("mach_id").toString,
    dbObject("work_id").toString,
    dbObject("CX").toString,
    dbObject("DX").toString,
    dbObject("LC").toString,
    dbObject("mach_status").toString,
    dbObject("product").toString,
    dbObject("insertDate").toString,
    dbObject("mac_address").toString,
    dbObject("shiftDate").toString
  )

  def getShiftTime(timestamp: Long) = {
    val offsetOf7Hours = 7 * 60 * 60 * 1000
    new Date((timestamp * 1000) - offsetOf7Hours)    
  }

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
      columns(9),
      columns(10),
      columns(11),
      columns(12),
      columns(13),
      MachineInfo.getProduct(machineID),
      dateFormatter.format(new Date(timestamp * 1000)),
      Try{columns(14)}.getOrElse(""),
      dateFormatter.format(getShiftTime(timestamp))
    )
  }
}
