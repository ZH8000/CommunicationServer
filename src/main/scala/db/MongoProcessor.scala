package tw.com.zhenhai.db

import tw.com.zhenhai.model._

import com.mongodb.casbah.Imports._

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import org.slf4j.LoggerFactory

object MachineStatusCode {
  /**
   *  機台狀態 01 = 生產中
   */
  val RUNNING = "01"

  /**
   *  機台狀態 02 = 進維修
   */
  val ENTER_MAINTAIN = "02"

  /**
   *  機台狀態 03 = 出維修
   */
  val EXIT_MAINTAIN = "03"

  /**
   *  機台狀態 04 = 生產完成
   */
  val PRODUCE_DONE = "04"

  /**
   *  機台狀態 05 = 機台鎖機（閒置過久）
   */
  val ENTER_LOCK = "05"

  /**
   *  機台狀態 06 = 機台解鎖
   */
  val EXIT_LOCK = "05"

  /**
   *  機台狀態 07 = 按下結單鈕強制結單，且良品數有到達目標值
   */
  val FORCE_CLOSE_FULFILL = "07"

  /**
   *  機台狀態 08 = 按下結單鈕強制結單，且良品數有到達目標值
   */
  val FORCE_CLOSE_NOT_FULFILL = "08"

  /**
   *  機台狀態 09 = 掃條碼
   */
  val SCAN_BARCODE = "09"

  /**
   *  機台狀態 10 = 機台 STANDBY，準備接受條碼輸入
   */
  val STANDBY = "10"

  /**
   *  機台斷電
   */
  val POWER_OFF = "11"

  /**
   *  機台開機完成，已撈完資料
   */
  val BOOT_COMPLETE  = "12"
}

/**
 *  此類別負責將從 RaspberryPi 送過來，已經轉換成 Scala 物件的資料，經過統計分析
 *  之後，分別存入不同的 MongoDB 資料表，以方便前台網站顯示時，不需要經過即時運算
 *  即可取出資料。
 *
 *
 *  @param    mongoClient   MongoDB 連線物件
 */
class MongoProcessor(mongoClient: MongoClient) {

  import MachineStatusCode._

  val zhenhaiDB = mongoClient("zhenhai")
  val zhenhaiDailyDB = mongoClient("zhenhaiDaily")
  val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

  /**
   *  新增或累計資料表內個 row 的事件數與良品數
   *
   *  這個函式會針對 tableName 進行 query，並且資料表內將符合該 query 的資料行取出，
   *  並且將其中的 event_qty 與 count_qty 欄位，分別加上傳入的 record 參數的
   *  event_qty 和 count_qty 的值。
   *
   *  如果查詢出來的 query 沒有符合的資料，則會在資料表中建立一個新的 row。
   *
   *  @param    tableName   要處理的資料表
   *  @param    query       用什麼樣的 query 來選取 row
   *  @param    record      新的資料
   */
  def update(tableName: String, query: MongoDBObject, record: Record) {
    val operation = $inc("event_qty" -> record.eventQty, "count_qty" -> record.countQty)
    zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
    zhenhaiDB(tableName).update(query, operation, upsert = true)
  }

  /**
   *  新增資料至異常資料警示資料表
   *
   *  由於 RaspberryPi 從生產機具取得的良品數有的時候會有異常的狀況產生，
   *  此時 RaspberryPi 傳入的資料，會將良品數 (count_qty) 欄位設成 -1，
   *  此種資料不應在統計時納入，所以會另外放在另一個資料表以供備查。
   *
   *  @param    record    要新增的機台記錄
   */
  def addMachineAlert(record: Record) {

    val query = MongoDBObject(
      "date"      -> record.tenMinute.substring(0, 10),
      "timestamp" -> record.tenMinute,
      "mach_id"   -> record.machID,
      "eventID"   -> record.eventID
    )

    zhenhaiDB("alert").update(query, query, upsert = true);
  }

  /**
   *  新增資料至網頁上「依人員」頁面的資料表中
   *
   *  此函式會將資料記錄至 workerDaily 資料表，以方便在網頁上「依人員」圖示
   *  點入後的報表顯示。
   *
   *  @param    record    目前處理的資料物件
   */
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

  /**
   *  新增資料至網頁上「人員效率」 Excel 頁面的資料表中
   *
   *  此函式會將資料記錄至 workerPerformance 資料表，用來計算「人員效率」
   *  的 Excel 報表。
   *
   *  @param    record    目前處理的資料物件
   */
  def updateWorkerPerformance(record: Record) {

    val month = record.shiftDate.substring(0, 7)
    val query = MongoDBObject(
      "shiftDate"     -> record.shiftDate,
      "workerMongoID" -> record.workID
    )

    zhenhaiDB(s"workerPerformance-$month").update(query, $inc("countQty" -> record.countQty), upsert = true)
    zhenhaiDB(s"workerPerformance-$month").ensureIndex(query.mapValues(x => 1))
  }



  /**
   *  記錄每一個工單號第一次出現的日期
   *
   *  這個函式會去查詢資料庫中的 lotDate 資料表中是否已經有傳入的 record 參數中的
   *  工單號 (lotNo) 的資料，如果沒有的話，則將此工號加入 lotDate 資料表中，並加上
   *  實際的生產日期和工班日期。
   *
   *  @param  record    目前處理的資料
   *
   */
  def updateLotToMonth(record: Record) {
    val lotDateTable = zhenhaiDB("lotDate")
    val existRecordHolder = lotDateTable.findOne(MongoDBObject("lotNo" -> record.lotNo))
    if (existRecordHolder.isEmpty) {
      lotDateTable.insert(
        MongoDBObject(
          "lotNo" -> record.lotNo, 
          "insertDate" -> record.insertDate.substring(0, 7), 
          "shiftDate" -> record.shiftDate.substring(0, 7)
        )
      )
    }
    zhenhaiDB("lotDate").ensureIndex(MongoDBObject("lotNo" -> 1))
  }


  /**
   *  更新機台累計生產量表格
   *
   *  此函式會將 machineCounter 資料表中相對應傳入的 record 的 machineID 的生產累計數。
   *
   *  @param  record    傳入的新資料
   */
  def updateMachineCounter(record: Record) {
    val machineCounter = zhenhaiDB("machineCounter")
    val operation = $inc("counter" -> record.countQty)
    machineCounter.update(MongoDBObject("machineID" -> record.machID), operation, true, false)
  }

  /**
   *  取得目前機台的累計生產數
   *
   *  @param      machineID   機台編號
   *  @return                 此機台目前的累計生產良品數數量
   */
  def getMachineCounter(machineID: String)  = {
    val machineCounter = zhenhaiDB("machineCounter")

    machineCounter.findOne(MongoDBObject("machineID" -> machineID))
                  .map(row => row("counter").toString.toLong)
                  .getOrElse(0L)
  }

  /**
   *  更新零件更換行事曆的資料表
   *
   *  此函式會依照零件更換的設定，更新 alarm 資料表，用來在網頁上
   *  顯示「零件更換行事曆」。
   *
   *  @param      record      目前要處理的生產資料
   */
  def updateAlarmStatus(record: Record) {
    val machineCounter = zhenhaiDB("machineCounter")
    val currentMachineCounter = getMachineCounter(record.machID)
    val alarmColl = zhenhaiDB("alarm")

    /**
     *  檢查是否該重設該維修行事曆的打勾狀態
     *
     *  當某個零件更新過，在網頁上被打勾後，alarm 資料表上相對應的 isDone 欄位會
     *  被設定為 true，但當上次打勾後的生產數量超過設定的數量時，isDone 欄位應該
     *  被設定為尚未打勾的狀態，因為此時零件已過了一個循環，需要再被更換。
     *
     *  @param  row   該維修行事曆的設定的 row
     *  @return       如果需要再次更換則為 true，否則為 false
     */
    def shouldReset(row: DBObject): Boolean = {
      val machineID = row.getAs[String]("machineID").getOrElse("")
      val lastUpdatedCount = row.getAs[Long]("lastReplaceCount").getOrElse(0L)
      val isDone = row.getAs[Boolean]("isDone").getOrElse(false)
      val countdownQty = row.getAs[Long]("countdownQty").getOrElse(0L)

      isDone && (countdownQty + lastUpdatedCount) <= currentMachineCounter
    }

    val updateTargetIDs = alarmColl.find(MongoDBObject("machineID" -> record.machID)).filter(shouldReset).map(_._id).flatten

    updateTargetIDs.foreach { alarmID =>
      val query = MongoDBObject("_id" -> alarmID)
      val operation = $set("isDone" -> false)
      alarmColl.update(query, operation)
    }
  }



  /**
   *  新增至鎖機記錄表
   *
   *  為了在「人員效率」 Excel 表中計算每個作業員每日的鎖機的總時間，必須要將
   *  鎖機的的狀況依照員工編號以及工班日期分類，並記錄到 lock 資料表
   *
   *  @param      record    要記錄的資料
   */
  def addToLockList(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val lockTable = zhenhaiDB(s"lock-$month")
    lockTable.insert(
      MongoDBObject(
        "lotNo" -> record.lotNo,
        "partNo" -> record.partNo,
        "workerMongoID" -> record.workID,
        "status" -> record.machineStatus,
        "machineID" -> record.machID,
        "timestamp" -> record.embDate,
        "shiftDate" -> record.shiftDate
      )
    )
  }

  /**
   *  更新 φ 別統計的資料表
   *
   *  @param    record    要處理的資料
   */
  def updateByProductSummary(record: Record) {
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
  }

  /**
   *  更新每日以十分鐘為間隔的統計資料
   *
   *  在系統中，會儲存每一天以及每一工班日的總結，以十分鐘為一個統計單位，
   *  用來顯示在網頁上統計資料最後一頁，下面的詳細生產資料總表。
   *
   *  @param    record    要處理的資料
   */
  def updateRecordByDate(record: Record) {
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
  }

  /**
   *  更新每日的錯誤統計資料
   *
   *  用來顯示在「錯誤分析」的資料
   *
   *  @param    record    要處理的資料
   */
  def updateDailyDefact(record: Record) {
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

  /**
   *  更新不良事件統計資料
   *
   *  topReason / reasonByMachine 兩個資料表用來顯示「本日前五大錯誤」和「錯誤分析」中的圓餅圖，
   *  defactByLotAndPart 用來顯示
   *
   *  @param    record    要處理的資料
   */
  def updateDefactReasonAndMachine(record: Record) {
    update(
      tableName = "topReason", 
      query = MongoDBObject(
        "mach_id"    -> record.machID,
        "mach_model" -> MachineInfo.getModel(record.machID),
        "defact_id"  -> record.defactID,
        "date"       -> record.insertDate,
        "shiftDate"  -> record.shiftDate,
        "machine_type" -> MachineInfo.getMachineTypeID(record.machID)
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

  /**
   *  更新 MongoDB 中的 dailyLossRate 資料表
   *
   *  此資料表用來實作「損耗查詢」頁面，此資料表會記錄每一工班日中的每一台機台，
   *  其製作的每一張工單的良品數以及應該被算入損耗的不良品數。
   *
   */
  def updateInputAndDefactByShiftDate(record: Record) {

    if (record.shouldCountAsLoss || record.countQty > 0) {

      val tableName = "dailyLossRate"
      val query = MongoDBObject(
        "shiftDate" -> record.shiftDate,
        "machineID" -> record.machID,
        "machineType" -> record.machineType,
        "partNo"    -> record.partNo
      )

      val operation = $inc("event_qty" -> record.eventQty, "count_qty" -> record.countQty)
      zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
      zhenhaiDB(tableName).update(query, operation, upsert = true)

    }
  }

  /**
   *  更新 MongoDB 中的 defactByLotNo 資料表
   *
   *  此資料表記錄了每張工作中，每一台所使用到的機台的良品數以及被視為
   *  損耗的不良品數。
   *
   */
  def updateInputAndDefactByLotNo(record: Record) {

    if (record.shouldCountAsLoss) {
      update(
        tableName = "defactByLotNo", 
        query = MongoDBObject(
          "lotNo"   -> record.lotNo,
          "mach_id" -> record.machID
        ), 
        record = record
      )
    }

    if (record.otherEventID == 0) {
      update(
        tableName = "inputByLotNo", 
        query = MongoDBObject(
          "lotNo"   -> record.lotNo,
          "mach_id" -> record.machID
        ), 
        record = record
      )
    }
  }

  /**
   *  更新其他事件統計資料表
   *
   *  僅做為參考資料，並未用在網頁顯示上
   *
   *  @param    record    要處理的資料
   */
  def updateOtherEvent(record: Record) {
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

  /**
   *  更新每日總計
   *
   *  此資料表用來顯示網頁上「產量統計」中的長條圖
   *
   *  @param    record    要處理的資料
   */
  def updateDailyRecord(record: Record) {

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
  }


  /*
   *  更新維修記錄表
   *
   *  此資料表用來顯示在網頁上的「維修記錄」頁面中
   *
   *  @param  record    要處理的資料
   */
  def updateMachineMaintainLog(record: Record) {
    zhenhaiDB("machineMaintainLog").insert(
      MongoDBObject(
        "workerMongoID"   -> record.workID,
        "timestamp"       -> record.embDate,
        "startTimestamp"  -> record.cxOrStartTimestamp,
        "maintenanceCode" -> record.eventID,
        "machineID"       -> record.machID,
        "status"          -> record.machineStatus,
        "insertDate"	    -> record.insertDate,
        "shiftDate"	    -> record.shiftDate
      )
    )
  }

  /**
   *  更新每台機台當日的累計良品數和事件數以及最新狀態代碼
   *
   *  @param  record  要處理的資料
   */
  def updateDailyMachineCount(record: Record) {
    val tableName = "dailyMachineCount"
    val query = MongoDBObject(
      "machineID"  -> record.machID,
      "insertDate" -> record.insertDate
    )

    update(tableName, query, record)
    val operation = $set("status" -> record.machineStatus)
    zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
    zhenhaiDB(tableName).update(query, operation)
  }

  /**
   *  更新每個工號的生產目標量
   *
   *  @param  record  要處理的資料
   */
  def updateWorkQty(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val tableName = s"workQty-$month"
    val operation = $set("workQty" -> record.workQty)
    val query = MongoDBObject("lotNo" -> record.lotNo)

    zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
    zhenhaiDB(tableName).update(query, operation, upsert = true)
    zhenhaiDB(tableName).update(query, $set("partNo" -> record.partNo))
  }

  /**
   *  更新「生產狀況」統計表中「卷取機」的資料表
   *
   *  @param  record  要處理的資料
   */
  def updateDefactSummaryStep1(record: Record) {

    val month = record.shiftDate.substring(0, 7)
    val tableName = s"defactSummary-$month"

    val query = MongoDBObject(
      "machineType" -> record.machineType,
      "machineID" -> record.machID,
      "machineModel" -> MachineInfo.getModel(record.machID),
      "shiftDate" -> record.shiftDate,
      "shift" -> record.shift,
      "area" -> record.area,
      "floor" -> record.floor,
      "product" -> record.fullProductCode
    )

    if (record.countQty > 0) {
      val operation = $inc("countQty" -> record.countQty)
      zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
      zhenhaiDB(tableName).update(query, operation, upsert = true)
    }

    val defactOperation = record.defactID match {
      case 0    => Some($inc("short" -> record.eventQty))
      case 3    => Some($inc("stick" -> record.eventQty))
      case 29   => Some($inc("tape" -> record.eventQty))
      case 1    => Some($inc("roll" -> record.eventQty))
      case _    => None

    }

    defactOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))

    val eventOperation = record.otherEventID match {
      case 201  => Some($inc("plus" -> record.eventQty))
      case 202  => Some($inc("minus" -> record.eventQty))
      case _ => None
    }

    eventOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))
  }

  /**
   *  更新「生產狀況」統計表中「組立機」的資料表
   *
   *  @param  record  要處理的資料
   */
  def updateDefactSummaryStep2(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val tableName = s"defactSummary-$month"

    val query = MongoDBObject(
      "machineType" -> record.machineType,
      "machineID" -> record.machID,
      "machineModel" -> MachineInfo.getModel(record.machID),
      "shiftDate" -> record.shiftDate,
      "shift" -> record.shift,
      "area" -> record.area,
      "floor" -> record.floor,
      "product" -> record.fullProductCode
    )

    if (record.countQty > 0) {
      val operation = $inc("countQty" -> record.countQty)
      zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
      zhenhaiDB(tableName).update(query, operation, upsert = true)
    }

    val defactOperation = record.defactID match {
      case 104  => Some($inc("defactD" -> record.eventQty))
      case 119  => Some($inc("white" -> record.eventQty))
      case _    => None

    }

    defactOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))

    val eventOperation = record.otherEventID match {
      case 103  => Some($inc("total" -> record.eventQty))
      case 105  => Some($inc("rubber" -> record.eventQty))
      case 106  => Some($inc("shell" -> record.eventQty))
      case _ => None
    }

    eventOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))
  }

  /**
   *  更新「生產狀況」統計表中「老化機」的資料表
   *
   *  @param  record  要處理的資料
   */
  def updateDefactSummaryStep3(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val tableName = s"defactSummary-$month"

    val query = MongoDBObject(
      "machineType" -> record.machineType,
      "machineID" -> record.machID,
      "machineModel" -> MachineInfo.getModel(record.machID),
      "shiftDate" -> record.shiftDate,
      "shift" -> record.shift,
      "area" -> record.area,
      "floor" -> record.floor,
      "product" -> record.fullProductCode
    )

    if (record.countQty > 0) {
      val operation = $inc("countQty" -> record.countQty)
      zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
      zhenhaiDB(tableName).update(query, operation, upsert = true)
    }


    val defactOperation = record.defactID match {
      case 202  => Some($inc("short" -> record.eventQty))
      case 201  => Some($inc("open" -> record.eventQty))
      case 205  => Some($inc("capacity" -> record.eventQty))
      case 206  => Some($inc("lose" -> record.eventQty))
      case 203  => Some($inc("lc" -> record.eventQty))
      case 207  => Some($inc("retest" -> record.eventQty))
      case _    => None
    }

    defactOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))

    val eventOperation = record.otherEventID match {
      case 0  => Some($inc("total" -> record.eventQty))
      case _ => None
    }

    eventOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))
  }

  /**
   *  更新「生產狀況」統計表中「TAPPING 機/ CUT 機」的資料表
   *
   *  @param  record  要處理的資料
   */
  def updateDefactSummaryStep5(record: Record) {
    val month = record.shiftDate.substring(0, 7)
    val tableName = s"defactSummary-$month"

    val query = MongoDBObject(
      "machineType" -> record.machineType,
      "machineID" -> record.machID,
      "machineModel" -> MachineInfo.getModel(record.machID),
      "shiftDate" -> record.shiftDate,
      "shift" -> record.shift,
      "area" -> record.area,
      "floor" -> record.floor,
      "product" -> record.fullProductCode
    )

    if (record.countQty > 0) {
      val operation = $inc("countQty" -> record.countQty)
      zhenhaiDB(tableName).ensureIndex(query.mapValues(x => 1))
      zhenhaiDB(tableName).update(query, operation, upsert = true)
    }

    val eventOperation = record.otherEventID match {
      case 0  => Some($inc("total" -> record.eventQty))
      case _ => None
    }

    eventOperation.foreach(o => zhenhaiDB(tableName).update(query, o, upsert = true))
  }


  /**
   *  依照傳入的資料來決定要更新「生產狀況」中的哪個資料表
   *
   *  @param  record    要處理的資料
   */
  def updateDefactSummary(record: Record) {
    record.machineType match {
      case 1 => updateDefactSummaryStep1(record)
      case 2 => updateDefactSummaryStep2(record)
      case 3 => updateDefactSummaryStep3(record)
      case 5 => updateDefactSummaryStep5(record)
      case _ =>
    }
  }



  /**
   *  將 RaspberryPi 送過來的資料處理分析
   *
   *  @param     要處理的資料
   */
  def addRecord(record: Record) {

    zhenhaiDailyDB(record.insertDate).insert(record.toMongoObject)

    updateDailyMachineCount(record)

    // 理論上每一筆生產資料的良品數或事件數不應該超過 2000，
    // 但有時機台會有異常訊號造成爆量。
    //
    // 當發生爆量的時候，將其記錄在 strangeQty 此資料表，以
    // 做為除錯時的備查資料。
    if (record.countQty >= 2000 || record.eventQty >= 2000) {
      zhenhaiDB("strangeQty").insert(record.toMongoObject)
    }

    val isRealData = 
      record.partNo != "0" && record.lotNo != "0" && 
      record.machineStatus != POWER_OFF && record.machineStatus != BOOT_COMPLETE

    if (isRealData) {
      updateWorkQty(record)
      updateDefactSummary(record)

      // 良品或不良事件
      if (record.countQty > 0 || record.defactID != -1) {
        updateByProductSummary(record)
        updateRecordByDate(record)
        updateDailyDefact(record)
        updateMachineCounter(record)
        updateAlarmStatus(record)
      }

      // 不良事件
      if (record.eventQty > 0 && record.defactID != -1) {
        updateDefactReasonAndMachine(record)
        updateInputAndDefactByLotNo(record)
      }

      // 其他統計事件
      if (record.otherEventID != -1) {
        updateOtherEvent(record)
        updateInputAndDefactByLotNo(record)
      }

      updateInputAndDefactByShiftDate(record)
      updateDailyRecord(record)

      if (record.machineStatus == ENTER_MAINTAIN || record.machineStatus == EXIT_MAINTAIN) {
        updateMachineMaintainLog(record)
      }

      if (record.machineStatus == ENTER_LOCK) {
        addToLockList(record)
      }

      if (record.isFromBarcode) {

        if (record.machineStatus != ENTER_MAINTAIN && record.machineStatus != EXIT_MAINTAIN) {
          updateWorkerDaily(record)
          updateWorkerPerformance(record)
        }

        updateLotToMonth(record)
      }
    }
  }
}
