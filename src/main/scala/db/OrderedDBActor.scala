package tw.com.zhenhai.db

import com.mongodb.casbah.Imports._
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import org.slf4j.LoggerFactory
import tw.com.zhenhai.model._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import com.rabbitmq.client.QueueingConsumer._

trait OrderedDBProcessor {
  def updateDB(record: Record): Unit
}

class UpdateProductionStatus extends OrderedDBProcessor {
  
  import MachineStatusCode._

  val mongoClient = MongoClient("localhost")
  val zhenhaiDB = mongoClient("zhenhai")

  implicit val logger = LoggerFactory.getLogger("DeQueueServer")

  /**
   *  新增資料至網頁上「今日工單」頁面的資料表中
   *
   *  此函式會將資料記錄至 productionStatus 資料表，用來顯示「今日工單」
   *  中的報表。
   *
   *  @param    record    目前處理的資料物件
   */
  def updateProductionStatus(record: Record) {
    val query = MongoDBObject(
      "partNo" -> record.partNo,
      "lotNo" -> record.lotNo,
      "product" -> record.product
    )

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
    zhenhaiDB("productionStatus").ensureIndex(query.mapValues(x => 1))
  }

  /**
   *  新增資料至網頁上「今日工單」的歷史資料資料表
   *
   *  @param    record    目前處理的資料物件
   */
  def updateProductionHistoryStatus(record: Record) {
    val query = MongoDBObject(
      "partNo" -> record.partNo,
      "lotNo" -> record.lotNo,
      "product" -> record.product,
      "shiftDate" -> record.shiftDate
    )

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

    val query = MongoDBObject(
      "partNo" -> record.partNo,
      "lotNo" -> record.lotNo,
      "product" -> record.product,
      "inputCount" -> record.workQty
    )

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


class UpdateOperationTime extends OrderedDBProcessor {
  
  import MachineStatusCode._

  val mongoClient = MongoClient("localhost")
  val zhenhaiDB = mongoClient("zhenhai")

  implicit val logger = LoggerFactory.getLogger("DeQueueServer")

  /**
   *  更新最新機台狀態
   *
   *  此資料表用在「機台狀況」的網頁上
   *
   *  @param    record      要處理的資料
   */
  def updateMachineStatus(record: Record) {

    val machineStatusTable = zhenhaiDB("machineStatus")
    val query = MongoDBObject("machineID" -> record.machID) 
    machineStatusTable.ensureIndex(query.mapValues(x => 1))

    zhenhaiDB("machineStatus").update(
      MongoDBObject("machineID" -> record.machID), 
      $set(
        "status" -> record.machineStatus,
        "lastUpdateTime" -> record.embDate
      ), 
      upsert = true
    )


  }

  /**
   *  更新「工單良品總數」資料表
   *
   *  當 Pi 的盒子斷電的時候，會傳送 POWER_OFF
   *
   *  @param    record    要處理的資料
   */
  def updateTotalCountOfOrders(record: Record) {
    val tableName = s"totalCountOfOrders"
    val totalCountTable = zhenhaiDB(tableName)

    val query = MongoDBObject(
      "lotNo"     -> record.lotNo,
      "partNo"    -> record.partNo,
      "machineID" -> record.machID
    )

    totalCountTable.ensureIndex(query.mapValues(x => 1))

    val operation1 = $inc("totalCount" -> record.countQty)
    val operation2 = $set("workQty" -> record.workQty)
    val operation3 = $set("status" -> record.machineStatus)

    totalCountTable.update(query, operation1, upsert = true)
    totalCountTable.update(query, operation2)
    totalCountTable.update(query, operation3)
  }

  /**
   *  取得某工單號第幾次刷條碼
   *
   *  目前的系統設計上，由於可能遇到停電、維修等異常狀況，所以一張工單不一定
   *  會只有刷一次的條碼。有可能第一次刷完條碼後，因為維修而暫時停工，需要再
   *  刷一次條碼進入生產程序。
   *
   *  為了能夠統計每張工單中每個作業員的實際生產時間，所以會有一個 operationTime
   *  資料表，記錄每個工單號刷了幾次。
   *
   *  這個函式會取回目前系統內特定工單號的刷條碼次數。
   *
   *  @param      record       要查詢的記錄
   *  @return                  這筆資料的工單號是第幾次被刷，若未被刷過則為 0
   *
   */
  def getLastOperationTimeIndex(record: Record): Long = {
    val month = record.shiftDate.substring(0, 7)
    val operationTime = zhenhaiDB(s"operationTime-$month")
    operationTime.count(
      MongoDBObject(
        "machineID"   -> record.machID,
        "lotNo"       -> record.lotNo,
        "partNo"      -> record.partNo,
        "productCode" -> record.fullProductCode,
        "shiftDate"   -> record.shiftDate,
        "workerID"    -> record.workID,
        "machineType" -> record.machineType
      )
    )
  }

  /**
   *  更新工單號最後的生產時間的序號
   *
   *  目前的系統設計上，由於可能遇到停電、維修等異常狀況，所以一張工單不一定
   *  會只有刷一次的條碼。有可能第一次刷完條碼後，因為維修而暫時停工，需要再
   *  刷一次條碼進入生產程序。
   *
   *  為了能夠統計每張工單中每個作業員的實際生產時間，所以會有一個 operationTime
   *  資料表，記錄每個工單號刷了幾次，以及每一個批次的最後生產記錄時間。
   *
   *  這個函式會更新工單號的最後生產時間。
   *
   *  @param      record       要處理的記錄物件
   */
  def incrementOperationTimeIndex(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val operationTime = zhenhaiDB(s"operationTime-$month")
    val lastIndex = getLastOperationTimeIndex(record)

    operationTime.insert(
      MongoDBObject(
        "machineID" -> record.machID,
        "lotNo" -> record.lotNo,
        "partNo" -> record.partNo,
        "productCode"   -> record.fullProductCode,
        "shiftDate"     -> record.shiftDate,
        "workerID" -> record.workID,
        "machineType" -> record.machineType,
        "startTimestamp" -> record.embDate,
        "order" -> (lastIndex + 1),
        "countQty" -> record.countQty,
        "currentTimestamp" -> record.embDate
      )
    )

    val query = MongoDBObject(
      "machineID" -> record.machID,
      "lotNo" -> record.lotNo,
      "partNo" -> record.partNo,
      "productCode"   -> record.fullProductCode,
      "shiftDate"     -> record.shiftDate,
      "workerID" -> record.workID,
      "machineType" -> record.machineType,
      "order" -> (lastIndex + 1)
    )

    operationTime.ensureIndex(query.mapValues(x => 1))

  }

  /**
   *  更新工單號最後的生產時間
   *
   *  @param    record    要處理的資料
   */
  def updateOperationTime(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val operationTime = zhenhaiDB(s"operationTime-$month")
    val lastIndex = getLastOperationTimeIndex(record)

    if (lastIndex == 0) {
      incrementOperationTimeIndex(record)
    } else {

      val query = MongoDBObject(
        "machineID" -> record.machID,
        "lotNo" -> record.lotNo,
        "partNo" -> record.partNo,
        "productCode"   -> record.fullProductCode,
        "shiftDate"     -> record.shiftDate,
        "workerID" -> record.workID,
        "machineType" -> record.machineType,
        "order" -> lastIndex
      )

      operationTime.ensureIndex(query.mapValues(x => 1))
      operationTime.update(query, $inc("countQty" -> record.countQty))
      operationTime.update(query, $set("currentTimestamp" -> record.embDate))
    }
  }

  def updateDB(record: Record) {

    updateMachineStatus(record)

    val isRealData = 
      record.partNo != "0" && record.lotNo != "0" && 
      record.machineStatus != POWER_OFF && record.machineStatus != BOOT_COMPLETE

    if (isRealData) {

      if (record.machineStatus == SCAN_BARCODE) {
        incrementOperationTimeIndex(record)
      }

      if (record.isFromBarcode) {
        updateOperationTime(record)
      }
    }

    updateTotalCountOfOrders(record)
  }
}
