package tw.com.zhenhai.db

import tw.com.zhenhai.model._

import com.mongodb.casbah.Imports._

import java.text.SimpleDateFormat
import java.util.Date

class MongoProcessor(mongoClient: MongoClient) {

  val zhenhaiDB = mongoClient("zhenhai")
  val dailyDB = mongoClient("zhenhaiDaily")
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

  def update(tableName: String, query: MongoDBObject, record: Record) {
    val operation = $inc("event_qty" -> record.eventQty, "count_qty" -> record.countQty)
    zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
    zhenhaiDB(tableName).update(query, operation, upsert = true)
  }

  def addMachineAlert(record: Record, isImportFromDaily: Boolean = false) {

    val query = MongoDBObject(
      "date"      -> record.tenMinute.substring(0, 10),
      "timestamp" -> record.tenMinute,
      "mach_id"   -> record.machID,
      "eventID"   -> record.eventID
    )

    zhenhaiDB("alert").update(query, query, upsert = true);
    if (!isImportFromDaily) {
      dailyDB(record.insertDate).insert(record.toMongoObject)
    }
  }

  def updateWorkerDaily(record: Record) {

    val query = MongoDBObject(
      "timestamp"     -> record.insertDate, 
      "shiftDate"     -> record.shiftDate,
      "workerMongoID" -> record.workID,
      "machineID"     -> record.machID
    )

    zhenhaiDB("workerDaily").update(query, $inc("countQty" -> record.countQty), upsert = true)
    zhenhaiDB("workerDaily").ensureIndex(query.mapValues(x => 1))
  }

  def updateDailyOrder(record: Record) {

    val query = MongoDBObject(
      "timestamp" -> record.insertDate,
      "shiftDate" -> record.shiftDate,
      "customer" -> record.customer,
      "partNo" -> record.partNo,
      "lotNo" -> record.lotNo,
      "product" -> record.product,
      "status" -> record.machineStatus
    )

    zhenhaiDB("dailyOrder").update(query, $inc("count_qty" -> record.countQty), upsert = true)
    zhenhaiDB("dailyOrder").ensureIndex(query.mapValues(x => 1))
  }

  def updateOrderStatus(record: Record) {

    //! Fix to real barcode data
    val timestamp = record.embDate
    val fieldName = MachineInfo.getMachineTypeID(record.machID) match {
      case 1 => "step1" // 加締
      case 2 => "step2" // 組立
      case 3 => "step3" // 老化
      case 4 => "step4" // 選別
      case 5 => "step5" // 加工切角
      case _ => "unknownStep"
    }

    val query = MongoDBObject(
      "partNo" -> record.partNo,
      "lotNo" -> record.lotNo,
      "product" -> record.product,
      "customer" -> record.customer,
      "inputCount" -> record.workQty
    )

    val orderStatusTable = zhenhaiDB("orderStatus")
    val existRecordHolder = orderStatusTable.findOne(MongoDBObject("lotNo" -> record.lotNo))
    val isNewStep = existRecordHolder match {
      case None => true
      case Some(oldRecord) => !oldRecord.keySet.contains(fieldName + "StartTime")
    }

    zhenhaiDB("orderStatus").update(query, $inc(fieldName -> record.countQty), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set("lastUpdated" -> record.embDate), upsert = true)

    if (record.machineStatus.trim == "04" || 
        record.machineStatus.trim == "06") {

      zhenhaiDB("orderStatus").update(query, $set(fieldName + "DoneTime" -> record.embDate), upsert = true)
    }

    if (isNewStep) {
      zhenhaiDB("orderStatus").update(query, $set(fieldName + "StartTime" -> record.embDate), upsert = true)
    }

    zhenhaiDB("orderStatus").update(query, $set(fieldName + "machineID" -> record.machID), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set(fieldName + "workerID" -> record.workID), upsert = true)
    zhenhaiDB("orderStatus").ensureIndex(query.mapValues(x => 1))
  }

  def addRecord(record: Record, isImportFromDaily: Boolean = false) {

    if (record.countQty >= 2000 || record.eventQty >= 2000) {
      zhenhaiDB("strangeQty").insert(record.toMongoObject)
    }

    update(
      tableName = "product", 
      query = MongoDBObject(
        "product" -> record.product,
        "machineType" -> record.machineType,
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
        "machineType" -> record.machineType,
        "capacityRange" -> record.capacityRange
      ), 
      record = record
    )

    // 良品或不良事件
    if (record.countQty > 0 || record.defactID != -1) {

      update(
        tableName = record.insertDate, 
        query = MongoDBObject(
          "timestamp" -> record.tenMinute, 
          "product"   -> record.product, 
          "mach_id"   -> record.machID, 
          "defact_id" -> record.defactID,
          "machineType" -> record.machineType,
          "capacityRange" -> record.capacityRange
        ), 
        record = record
      )

      update(
        tableName = s"shift-${record.shiftDate}", 
        query = MongoDBObject(
          "timestamp" -> record.tenMinute, 
          "product"   -> record.product, 
          "mach_id"   -> record.machID, 
          "defact_id" -> record.defactID,
          "machineType" -> record.machineType,
          "capacityRange" -> record.capacityRange
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
    }

    // 不良事件
    if (record.eventQty > 0 && record.defactID != -1) {
      update(
        tableName = "topReason", 
        query = MongoDBObject(
          "mach_id"    -> record.machID,
          "mach_model" -> MachineInfo.getModel(record.machID),
          "defact_id"  -> record.defactID,
          "date"       -> record.insertDate,
          "shiftDate"  -> record.shiftDate
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

    }

    // 其他統計事件
    if (record.otherEventID != -1) {
      update(
        tableName = s"event-${record.insertDate}", 
        query = MongoDBObject(
          "timestamp" -> record.tenMinute, 
          "product"   -> record.product, 
          "mach_id"   -> record.machID, 
          "other_event_id" -> record.otherEventID,
          "machineType" -> record.machineType,
          "capacityRange" -> record.capacityRange
        ), 
        record = record
      )

      update(
        tableName = s"event-shift-${record.shiftDate}", 
        query = MongoDBObject(
          "timestamp" -> record.tenMinute, 
          "product"   -> record.product, 
          "mach_id"   -> record.machID, 
          "other_event_id" -> record.otherEventID,
          "machineType" -> record.machineType,
          "capacityRange" -> record.capacityRange
        ), 
        record = record
      )

      update(
        tableName = "dailyEvent", 
        query = MongoDBObject(
          "timestamp" -> record.insertDate, 
          "shiftDate" -> record.shiftDate, 
          "mach_id"   -> record.machID, 
          "other_event_id" -> record.otherEventID
        ), 
        record = record
      )
    }

    update(
      tableName = "daily", 
      query = MongoDBObject(
        "timestamp" -> record.insertDate, 
        "shiftDate" -> record.shiftDate, 
        "mach_id"   -> record.machID,
        "machineType" -> record.machineType,
        "capacityRange" -> record.capacityRange
      ), 
      record = record
    )

    if (record.isFromBarcode) {
      updateWorkerDaily(record)
      updateDailyOrder(record)
      updateOrderStatus(record)
    }

    if (!isImportFromDaily) {
      dailyDB(record.insertDate).insert(record.toMongoObject)
    }
  }
}
