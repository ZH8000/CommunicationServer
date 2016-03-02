package tw.com.zhenhai.db

import com.mongodb.casbah.Imports._
import tw.com.zhenhai.model._
import org.slf4j.LoggerFactory

class UpdateOperationTime extends OrderedDBProcessor {
  
  import MachineStatusCode._

  val mongoClient = MongoClient("localhost")
  val zhenhaiDB = mongoClient("zhenhai")

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

  def updateDailyLotNoPartNo(record: Record) {
    val dailyLotNoPartNo = zhenhaiDB("dailyLotNoPartNo")
    val query = MongoDBObject(
      "machineID" -> record.machID,
      "lotNo"     -> record.lotNo,
      "partNo"    -> record.partNo,
      "shiftDate" -> record.shiftDate
    )

    dailyLotNoPartNo.ensureIndex(query.mapValues(x => 1))
    dailyLotNoPartNo.update(query, $set("lastUpdated" -> record.embDate), upsert = true)
    dailyLotNoPartNo.update(query, $set("lastStatus" -> record.machineStatus), upsert = true)
  }

  /**
   *  更新「工單良品總數」資料表
   *
   *  當 Pi 的盒子斷電的時候，會傳送 POWER_OFF 訊號，以告知 Server 有不正常斷電之
   *  情況。當 Pi 盒子開機時，會向 Server 索取資料，而 Server 會把該機台上狀最後
   *  一筆資料的狀態為 POWER_OFF 的訂單良品累計總數與目標量回傳。
   *
   *  此函式會更新 totalCounOfOrders 資料表，並記錄各筆訂單目前累計的良品總數，以
   *  及該筆訂單最後的狀態，以供 RaspberryPi 開機時查詢之用。
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
    val operation4 = $set("lastUpdatedTime" -> record.embDate)

    totalCountTable.update(query, operation1, upsert = true)
    totalCountTable.update(query, operation2)
    totalCountTable.update(query, operation3)
    totalCountTable.update(query, operation4)
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

  /**
   *  依據 RaspberryPi 傳回的資料更新資料庫
   *
   *  @param    record      要處理的資料
   */
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
        updateDailyLotNoPartNo(record)
      }
    }

    updateTotalCountOfOrders(record)
  }
}
