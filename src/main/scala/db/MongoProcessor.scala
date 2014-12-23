package tw.com.zhenhai.db

import tw.com.zhenhai.model._

import com.mongodb.casbah.Imports._

import java.text.SimpleDateFormat
import java.util.Date

class MongoProcessor(mongoClient: MongoClient) {

  val zhenhaiDB = mongoClient("zhenhai")
  val dailyDB = mongoClient("zhenhaiDaily")
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  def update(tableName: String, query: MongoDBObject, record: Record) {
    val operation = $inc("bad_qty" -> record.badQty, "count_qty" -> record.countQty)
    zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
    zhenhaiDB(tableName).update(query, operation, upsert = true)
  }

  def addMachineAlert(record: Record, isImportFromDaily: Boolean = false) {

    val tenMinute = dateFormatter.format(record.embDate * 1000).substring(0, 15) + "0"

    val query = MongoDBObject(
      "date"      -> tenMinute.substring(0, 10),
      "timestamp" -> tenMinute,
      "mach_id"   -> record.machID,
      "defact_id" -> record.defactID
    )

    zhenhaiDB("alert").update(query, query, upsert = true);
    if (!isImportFromDaily) {
      dailyDB(record.insertDate).insert(record.toMongoObject)
    }
  }

  def updateWorkerDaily(record: Record) {

    //! Fix to real barcode data
    val workerMongoID = record.workID

    val query = MongoDBObject(
      "timestamp"     -> dateFormatter.format(record.embDate * 1000).substring(0, 10), 
      "workerMongoID" -> workerMongoID,
      "machineID"     -> record.machID
    )

    zhenhaiDB("workerDaily").update(query, $inc("count_qty" -> record.countQty), upsert = true)
    zhenhaiDB("workerDaily").ensureIndex(query.mapValues(x => 1))
  }

  def updateDailyOrder(record: Record) {

    //! Fix to real barcode data
    val timestamp = dateFormatter.format(record.embDate * 1000).substring(0, 10)
    val lotNo = record.lotNo
    val order = record.orderType
    val customer = ""
    val status = record.machineStatus

    val query = MongoDBObject(
      "timestamp" -> timestamp,
      "lotNo" -> lotNo,
      "order" -> order,
      "customer" -> customer,
      "product" -> record.product,
      "status" -> status
    )

    zhenhaiDB("dailyOrder").update(query, $inc("count_qty" -> record.countQty), upsert = true)
    zhenhaiDB("dailyOrder").ensureIndex(query.mapValues(x => 1))
  }

  def updateOrderStatus(record: Record) {

    //! Fix to real barcode data
    val timestamp = record.embDate
    val order = record.orderType
    val customer = ""
    val fieldName = MachineInfo.getMachineTypeID(record.machID) match {
      case 1 => "step1"
      case 2 => "step2"
      case 3 => "step3"
      case 4 => "step4"
      case 5 => "step5"
      case 6 => "step6"
      case _ => "unknownStep"
    }

    val query = MongoDBObject(
      "customer" -> customer,
      "order" -> order,
      "product" -> record.product,
      "inputCount" -> record.workQty
    )

    zhenhaiDB("orderStatus").update(query, $inc(fieldName -> record.countQty), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set("lastUpdated" -> record.embDate), upsert = true)
    zhenhaiDB("orderStatus").ensureIndex(query.mapValues(x => 1))
  }

  def addRecord(record: Record, isImportFromDaily: Boolean = false) {
    val tenMinute = dateFormatter.format(record.embDate * 1000).substring(0, 15) + "0"

    update(
      tableName = "product", 
      query = MongoDBObject(
        "product" -> record.product,
        "machineTypeTitle" -> record.machineTypeTitle,
        "capacityRange" -> record.capacityRange
      ), 
      record = record
    )

    update(
      tableName = s"product-${record.product}", 
      query = MongoDBObject(
        "timestamp" -> record.insertDate, 
        "shiftDate" -> record.shiftDate, 
        "mach_id" -> record.machID,
        "machineTypeTitle" -> record.machineTypeTitle,
        "capacityRange" -> record.capacityRange
      ), 
      record = record
    )

    update(
      tableName = record.insertDate, 
      query = MongoDBObject(
        "timestamp" -> tenMinute, 
        "product"   -> record.product, 
        "mach_id"   -> record.machID, 
        "defact_id" -> record.defactID
      ), 
      record = record
    )

    update(
      tableName = s"shift-${record.shiftDate}", 
      query = MongoDBObject(
        "timestamp" -> tenMinute, 
        "product"   -> record.product, 
        "mach_id"   -> record.machID, 
        "defact_id" -> record.defactID
      ), 
      record = record
    )

    update(
      tableName = "dailyDefact", 
      query = MongoDBObject(
        "timestamp" -> record.insertDate, 
        "shiftDate" -> record.shiftDate, 
        "mach_id"   -> record.machID, 
        "defact_id" -> record.defactID
      ), 
      record = record
    )

    update(
      tableName = "daily", 
      query = MongoDBObject(
        "timestamp" -> record.insertDate, 
        "shiftDate" -> record.shiftDate, 
        "mach_id"   -> record.machID,
        "machineTypeTitle" -> record.machineTypeTitle,
        "capacityRange" -> record.capacityRange
      ), 
      record = record
    )

    update(
      tableName = "reasonByMachine", 
      query = MongoDBObject(
        "mach_id"    -> record.machID,
        "mach_model" -> MachineInfo.getModel(record.machID),
        "mach_type"  -> MachineInfo.getMachineType(record.machID)
      ), 
      record = record
    )

    updateWorkerDaily(record)
    updateDailyOrder(record)
    updateOrderStatus(record)

    if (!isImportFromDaily) {
      dailyDB(record.insertDate).insert(record.toMongoObject)
    }
  }
}


