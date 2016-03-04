package tw.com.zhenhai.db

import com.mongodb.casbah.Imports._
import org.slf4j.LoggerFactory
import tw.com.zhenhai.model._

class UpdateProductionStatus extends OrderedDBProcessor {
  
  import MachineStatusCode._

  val mongoClient = MongoClient("localhost")
  val zhenhaiDB = mongoClient("zhenhai")

  /**
   *  新增資料至網頁上「今日工單」頁面的資料表中
   *
   *  此函式會將資料記錄至 productionStatus 資料表，用來顯示「今日工單」
   *  中的報表。
   *
   *  @param    record    目前處理的資料物件
   */
  def updateProductionStatus(record: Record) {
    val query = MongoDBObject("lotNo" -> record.lotNo)
    val fieldName = MachineInfo.getMachineTypeID(record.machID) match {
      case 1 => "step1" // 加締
      case 2 => "step2" // 組立
      case 3 => "step3" // 老化
      case 4 => "step4" // 選別
      case 5 => "step5" // 加工切角
      case _ => "unknownStep"
    }

    zhenhaiDB("productionStatus").update(query, $set("lastUpdated" -> record.insertDate), upsert = true)
    zhenhaiDB("productionStatus").update(query, $set("lastUpdatedShifted" -> record.shiftDate))
    zhenhaiDB("productionStatus").update(query, $set(fieldName + "Status" -> record.machineStatus))
    zhenhaiDB("productionStatus").update(query, $set(fieldName + "Machine" -> record.machID))
    zhenhaiDB("productionStatus").update(query, $set(fieldName + "Count" -> record.countQty))
    zhenhaiDB("productionStatus").update(query, $set("workQty" -> record.workQty))
    zhenhaiDB("productionStatus").update(query, $set("partNo" -> record.partNo))
    zhenhaiDB("productionStatus").ensureIndex(query.mapValues(x => 1))
  }

  /**
   *  新增資料至網頁上「今日工單」的歷史資料資料表
   *
   *  @param    record    目前處理的資料物件
   */
  def updateProductionHistoryStatus(record: Record) {
    val query = MongoDBObject("lotNo" -> record.lotNo, "shiftDate" -> record.shiftDate)
    val fieldName = MachineInfo.getMachineTypeID(record.machID) match {
      case 1 => "step1" // 加締
      case 2 => "step2" // 組立
      case 3 => "step3" // 老化
      case 4 => "step4" // 選別
      case 5 => "step5" // 加工切角
      case _ => "unknownStep"
    }

    zhenhaiDB("productionStatusHistory").update(query, $set(fieldName + "Status" -> record.machineStatus), upsert = true)
    zhenhaiDB("productionStatusHistory").update(query, $set(fieldName + "Machine" -> record.machID))
    zhenhaiDB("productionStatusHistory").update(query, $set(fieldName + "Count" -> record.countQty))
    zhenhaiDB("productionStatusHistory").update(query, $set(fieldName + "Count" -> record.countQty))
    zhenhaiDB("productionStatusHistory").update(query, $set("workQty" -> record.workQty))
    zhenhaiDB("productionStatusHistory").update(query, $set("partNo" -> record.partNo))
    zhenhaiDB("productionStatusHistory").ensureIndex(query.mapValues(x => 1))
  }


  /**
   *  新增資料至網頁上「工單狀態」頁面的資料表中
   *
   *  此函式會將資料記錄至 orderStatus 資料表，用來顯示「工單狀態」
   *  中的報表。
   *
   *  @param    record    目前處理的資料物件
   */
  def updateOrderStatus(record: Record) {

    val fieldName = MachineInfo.getMachineTypeID(record.machID) match {
      case 1 => "step1" // 加締
      case 2 => "step2" // 組立
      case 3 => "step3" // 老化
      case 4 => "step4" // 選別
      case 5 => "step5" // 加工切角
      case _ => "unknownStep"
    }

    val query = MongoDBObject("lotNo" -> record.lotNo)

    val orderStatusTable = zhenhaiDB("orderStatus")
    val lotDateTable = zhenhaiDB("lotDate")
    val lotDateRecord = lotDateTable.findOne(MongoDBObject("lotNo" -> record.lotNo))
    val existRecordHolder = orderStatusTable.findOne(MongoDBObject("lotNo" -> record.lotNo))
    val isNewStep = existRecordHolder match {
      case None => true
      case Some(oldRecord) => !oldRecord.keySet.contains(fieldName + "StartTime")
    }

    zhenhaiDB("orderStatus").update(query, $inc(fieldName -> record.countQty), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set("lastUpdated" -> record.embDate), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set("customer" -> record.customer), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set("partNo" -> record.partNo), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set("inputCount" -> record.workQty), upsert = true)

    lotDateRecord.foreach { lotDate =>
      zhenhaiDB("orderStatus").update(query, $set("insertDate" -> lotDate("insertDate")), upsert = true)
      zhenhaiDB("orderStatus").update(query, $set("shiftDate" -> lotDate("shiftDate")), upsert = true)
    }

    if (record.machineStatus == PRODUCE_DONE || 
        record.machineStatus == FORCE_CLOSE_FULFILL) {

      zhenhaiDB("orderStatus").update(query, $set(fieldName + "DoneTime" -> record.embDate), upsert = true)
    }

    if (isNewStep) {
      zhenhaiDB("orderStatus").update(query, $set(fieldName + "StartTime" -> record.embDate), upsert = true)
    }

    zhenhaiDB("orderStatus").update(query, $set(fieldName + "machineID" -> record.machID), upsert = true)
    zhenhaiDB("orderStatus").update(query, $set(fieldName + "workerID" -> record.workID), upsert = true)
    zhenhaiDB("orderStatus").ensureIndex(query.mapValues(x => 1))
  }


  /**
   *  依據 RaspberryPi 傳回的資料更新資料庫
   *
   *  @param    record      要處理的資料
   */
  def updateDB(record: Record) {

    val isRealData = 
      record.partNo != "0" && record.lotNo != "0" && 
      record.machineStatus != POWER_OFF && record.machineStatus != BOOT_COMPLETE

    if (isRealData) {

      if (record.isFromBarcode) {
        updateOrderStatus(record)			// Peformance bottle neck
        updateProductionStatus(record)
        updateProductionHistoryStatus(record)
      }
    }
  }
}



