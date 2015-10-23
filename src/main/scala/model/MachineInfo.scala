package tw.com.zhenhai.model

import java.net.InetAddress
import scala.io.Source

/**
 *  此 Singleton 物件儲存了關於生產機台的資訊，包括每台機台的編號，型號，IP 位置等，
 *  以及如何從機台本身的事件訊號代碼，轉換成整個網站的統一事件代碼。
 *
 */
object MachineInfo {

  /**
   *  根據伺服器的 Hostname 取得機台設定檔的前綴
   *
   *  若是謝蘇州廠則為 sz，若為謝崗廠則為 xg
   */
  val csvPrefix = {

    val hostname = InetAddress.getLocalHost().getHostName()

    hostname match {
      case _ => "/xg"
    }
  }

  /**
   *  用來代表生產機台資訊的物件
   *
   *  @param    ip            機台的 IP 位址
   *  @param    machineID     機台編號
   *  @param    machineType   機台的制程（加締、組立、老化、加工……）
   *  @param    model         機台型號
   *  @param    note          備註
   */
  case class MachineInfo(ip: String, machineID: String, machineType: Int, model: String, note: Option[String])

  /**
   *  機台實體位置資料
   *
   *  @param    floor   所在樓層
   *  @param    area    所在區域
   */
  case class AreaInfo(floor: Int, area: String)

  /**
   *  統一計數事件 ID 對照表，型式為：
   *
   *  "機台型號" -＞ Map（原始事件ID -＞ 統一計數事件 ID）
   *
   *  關於詳細的 ID 意義，請參照系統設計說明文件
   */
  val otherEventTable = Map(
    // 原始事件 ID -> 統整過後的統一計數事件 ID
    "CAS-3000SA" -> Map(8  -> 0),
    "CS-205"     -> Map(17 -> 0),
    "CS-210"     -> Map(17 -> 0),
    "CS-204K"    -> Map(17 -> 0),
    "CS-206K"    -> Map(17 -> 0),
    "CS-206"     -> Map(17 -> 0),
    "CS-223"     -> Map(17 -> 0),
    "ATS-100M"   -> Map(9  -> 0),
    "ATS-110M"   -> Map(9  -> 0),
    "ATS-161H"   -> Map(9  -> 0),
    "ATS-600"    -> Map(9  -> 0),
    "ATS-600M"   -> Map(9  -> 0),
    "ATS-630J"   -> Map(9  -> 0),
    "ATS-720"    -> Map(9  -> 0),
    "ATS-900"    -> Map(9  -> 0),
    "NCR-236A"   -> Map(7  -> 0),
    "NCR-356B"   -> Map(7  -> 0),
    "FTO-2200"   -> Map(8  -> 101, 9 -> 103),
    "FTO-2200A"  -> Map(8  -> 103, 18 -> 105, 19 -> 106, 20 -> 107, 21 -> 108),
    "FTO-2400"   -> Map(2  -> 103, 22 -> 105, 23 -> 106, 24 -> 107),
    "FTO-2500"   -> Map(2  -> 103, 22 -> 105, 23 -> 106, 24 -> 107),
    "FTO-2510"   -> Map(2  -> 103, 22 -> 105, 23 -> 106, 24 -> 107),
    "FTO-3000"   -> Map(2  -> 103, 22 -> 105, 23 -> 106, 24 -> 107),
    "TIA-2600"   -> Map(2  -> 103, 3 -> 101),
    "FTO-2700"   -> Map(26 -> 103, 31 -> 108, 29 -> 107, 32 -> 106, 33 -> 105),
    "FTO-3100"   -> Map(26 -> 103, 31 -> 108, 29 -> 107, 32 -> 106, 33 -> 105),
    "FTO-3050"   -> Map(1 -> 101, 5 -> 102, 4 -> 103),
    "TSW-100T"   -> Map(10 -> 201, 14 -> 202),
    "HSW-500"    -> Map(3 -> 201, 2 -> 202),
    "HSW-800"    -> Map(3 -> 201, 2 -> 202),
    "ACG-308S-H" -> Map(3 -> 0),
    "ACG-508F"   -> Map(3 -> 0)
  )

  /**
   *  統一錯誤事件 ID 對照表，型式為：
   *
   *  "機台型號" -＞ Map（原始事件ID -＞ 統一錯誤事件 ID）
   *
   *  關於詳細的 ID 意義，請參照系統設計說明文件
   */
  val defactEventTable = Map(
    // 原始事件 ID -> 統整過後的統一錯誤事件 ID
    "TSW-100T" -> Map(
         9   ->    0,
         21  ->    1,
         22  ->    2,
         23  ->    3,
         3   ->    4,
         5   ->    5,
         6   ->    6,
         4   ->    7,
         7   ->    8,
         24  ->   11,
         25  ->   12,
         26  ->   13,
         27  ->   14,
         36  ->   15,
         29  ->   16,
         32  ->   17,
         30  ->   18,
         28  ->   20,
         31  ->   22,
         38  ->   23,
         39  ->   24,
         40  ->   25,
         41  ->   26,
         18  ->   27,
         2   ->   28,
         22  ->   29
    ),
    "HSW-500" -> Map(
         14  ->   0,
         15  ->   1,
         27  ->   3,
         32  ->   4, 
         33  ->   5, 
         13  ->   6, 
         30  ->   7, 
         31  ->   8, 
         29  ->  11, 
         25  ->  15, 
         24  ->  16, 
         19  ->  17, 
         18  ->  18, 
         12  ->  19, 
         21  ->  20, 
         11  ->  21, 
         20  ->  22, 
         22  ->  24, 
         23  ->  26, 
         28  ->  27, 
         14  ->  28 
    ),
    "HSW-800" -> Map(
         14  ->   0,
         15  ->   1,
         27  ->   3,
         32  ->   4, 
         33  ->   5, 
         13  ->   6, 
         30  ->   7, 
         31  ->   8, 
         29  ->  11, 
         25  ->  15, 
         24  ->  16, 
         19  ->  17, 
         18  ->  18, 
         12  ->  19, 
         21  ->  20, 
         11  ->  21, 
         20  ->  22, 
         22  ->  24, 
         23  ->  26, 
         28  ->  27, 
         14  ->  28 
    ),
    "TSW-168T" -> Map(
         3   ->  0,
         4   ->  1,
         2   ->  2,
         32  ->  3,
         7   ->  4,
         15  ->  5,
         26  ->  6,
         6   ->  7,
         14  ->  8,
         27  -> 11,
         25  -> 13,
         24  -> 14,
         12  -> 16,
         19  -> 19,
         18  -> 20,
         11  -> 21,
         10  -> 22,
         34  -> 27,
         35  -> 28,
         33  -> 29
    ),
    "TSW-303" -> Map(
         25  -> 0,
         3   -> 1,
         22  -> 2,
         11  -> 4,
         12  -> 5,
         16  -> 7,
         9   -> 8,
         10  -> 15,
         15  -> 16,
         14  -> 23,
         8   -> 24,
         7   -> 25,
         13  -> 26,
         20  -> 30,
         21  -> 31,
         24  -> 32,
         26  -> 33,
         19  -> 34
    ),
    "SPH-3000" -> Map(
         2   -> 0,
         4   -> 4,
         3   -> 5,
         8   -> 7,
         7   -> 8,
         9   -> 24,
        10   -> 26
    ),
    "FTO-2200A" -> Map(
         27  -> 101,
         39  -> 102,
         37  -> 103,
         34  -> 104,
         12  -> 105,
         13  -> 106,
         14  -> 107,
         15  -> 108,
         26  -> 110,
         28  -> 111,
         29  -> 112,
         10  -> 113,
         30  -> 114,
         32  -> 116,
         33  -> 117,
         31  -> 118,
         20  -> 119,
         35  -> 122,
         36  -> 123,
         38  -> 125,
         17  -> 126
    ),
    "FTO-2400" -> Map(
        6    ->  101,
        7    ->  102,
        8    ->  103,
        9    ->  104,
        20   ->  105,
        19   ->  107,
        11   ->  114,
        10   ->  115,
        16   ->  117,
        17   ->  118,
        24   ->  119,
        12   ->  120,
        21   ->  121,
        13   ->  123,
        14   ->  124,
        18   ->  125
    ),
    "FTO-2500" -> Map(
        6    ->  101,
        7    ->  102,
        8    ->  103,
        9    ->  104,
        20   ->  105,
        19   ->  107,
        11   ->  114,
        10   ->  115,
        16   ->  117,
        17   ->  118,
        24   ->  119,
        12   ->  120,
        21   ->  121,
        13   ->  123,
        14   ->  124,
        18   ->  125
    ),
    "FTO-2510" -> Map(
        6    ->  101,
        7    ->  102,
        8    ->  103,
        9    ->  104,
        20   ->  105,
        19   ->  107,
        11   ->  114,
        10   ->  115,
        16   ->  117,
        17   ->  118,
        24   ->  119,
        12   ->  120,
        21   ->  121,
        13   ->  123,
        14   ->  124,
        18   ->  125
    ),
    "FTO-3000" -> Map(
        6    ->  101,
        7    ->  102,
        8    ->  103,
        9    ->  104,
        20   ->  105,
        19   ->  107,
        11   ->  114,
        10   ->  115,
        16   ->  117,
        17   ->  118,
        24   ->  119,
        12   ->  120,
        21   ->  121,
        13   ->  123,
        14   ->  124,
        18   ->  125
    ),
    "TIA-2600" -> Map(
        56   ->  101,
        52   ->  102,
        62   ->  103,
        53   ->  104,
        38   ->  105,
        37   ->  106,
        39   ->  107,
        36   ->  108,
        51   ->  109,
        55   ->  110,
        43   ->  111,
        58   ->  112,
        59   ->  114,
        40   ->  115,
        50   ->  117,
        46   ->  118,
        63   ->  122,
        64   ->  123,
        57   ->  124,
        54   ->  125
    ),
    "FTO-2700" -> Map(
        24   ->  101,
        3    ->  102,
        2    ->  103,
        5    ->  104,
        11   ->  105,
        12   ->  107,
        13   ->  108,
        7    ->  110,
        9    ->  111,
        8    ->  112,
        10   ->  113,
        14   ->  114,
        15   ->  116,
        17   ->  117,
        16   ->  118,
        4    ->  122,
        6    ->  123,
        25   ->  126
    ),
    "FTO-3100" -> Map(
        24   ->   101,
        3    ->   102,
        2    ->   103,
        5    ->   104,
        11   ->   105,
        12   ->   107,
        13   ->   108,
        7    ->   110,
        9    ->   111,
        8    ->   112,
        10   ->   113,
        14   ->   114,
        15   ->   116,
        4    ->   122,
        6    ->   123,
        25   ->   126
    ),
    "FTO-3050" -> Map(
        34   ->   101,
        38   ->   102,
        37   ->   103,
        22   ->   105,
        20   ->   107,
        23   ->   108,
        7    ->   110,
        16   ->   111,
        29   ->   112,
        19   ->   113,
        28   ->   114,
        26   ->   115,
        30   ->   121,
        12   ->   122,
        27   ->   124,
        8    ->   125,
        9    ->   126
    ),
    "GT-480P" -> Map(
        4   ->    203,
        2   ->    205,
        3   ->    206
    ),
    "GT-1318P" -> Map(
        4   ->    203,
        2   ->    205,
        3   ->    206
    ),
    "CAS-3000SA" -> Map(
       17   ->   201,
       12   ->   202,
       13   ->   203,
       15   ->   205,
       14   ->   206,
       7    ->   207
    ),
    "CS-205" -> Map(
       10   ->   201,
       13   ->   202,
       12   ->   203,
       15   ->   205,
       14   ->   206,
       11   ->   207
    ),
    "CS-210" -> Map(
       10   ->   201,
       13   ->   202,
       12   ->   203,
       15   ->   205,
       14   ->   206,
       11   ->   207
    ),
    "CS-204K"    -> Map(
       10   ->   201,
       13   ->   202,
       12   ->   203,
       15   ->   205,
       14   ->   206,
       11   ->   207
    ),
    "CS-206K"    -> Map(
       10   ->   201,
       13   ->   202,
       12   ->   203,
       15   ->   205,
       14   ->   206,
       11   ->   207
    ),
    "CS-206"     -> Map(
       10   ->   201,
       13   ->   202,
       12   ->   203,
       15   ->   205,
       14   ->   206,
       11   ->   207
    ),
    "CS-223"     -> Map(
       10   ->   201,
       13   ->   202,
       12   ->   203,
       15   ->   205,
       14   ->   206,
       11   ->   207
    ),
    "ATS-100M"   -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-110M"   -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-161H"   -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-600"    -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-600M"   -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-630J"   -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-720"    -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ATS-900"    -> Map(
       12   ->   201,
       11   ->   202,
       15   ->   203,
       14   ->   204,
       16   ->   205,
       13   ->   206,
       10   ->   208
    ),
    "ACG-308S-H"  -> Map(
        5   ->   201,
        6   ->   202,
       10   ->   205,
       11   ->   203,
       12   ->   204,
       13   ->   206,
       14   ->   207
    ),
    "ACG-508F"    -> Map(
        5   ->   201,
        6   ->   202,
       10   ->   205,
       11   ->   203,
       12   ->   204,
       13   ->   206,
       14   ->   207
    )
  )

  /**
   *  機台編號與生產 φ 別的對應
   *
   *  此為尚未實裝條碼系統時所使用，若已確認所有條碼裝已實裝，應將此
   *  對照表刪除。
   */
  val productMapping: Map[String, String] = {
    var mapping: Map[String, String] = Map.empty
    val resourceStream = getClass.getResource(csvPrefix + "ProductMapping.csv").openStream()
    val csvFile = Source.fromInputStream(resourceStream)

    csvFile.getLines.foreach { line =>
      val Array(machineID, productSize) = line.split("\\|")
      mapping += (machineID -> productSize)
    }

    mapping
  }

  /**
   *  製程代號與製程名稱的對應
   */
  val machineTypeName = Map(
    1 -> "加締卷取",  // 機台編號字首為 E 的機台
    2 -> "組立",      // 機台編號字首為 G 的機台
    3 -> "老化",      // 機台編號字首為 A 的機台
    4 -> "選別",      // 機台編號字首為 A 的左邊四台機台
    5 -> "加工切角",  // 機台編號字首為 T, C 的機台
    6 -> "包裝"       // 機台編號字首為 B 的機台
  )

  /**
   *  機台列表，列出現存的所有機台
   */
  lazy val machineList: List[MachineInfo] = {

    val resourceStream = getClass.getResource(csvPrefix + "MachineList.csv").openStream()
    val csvFile = Source.fromInputStream(resourceStream)

    csvFile.getLines.toList.map { line =>
      val cols = line.split("\\|")
      val ip = cols(0)
      val machineID = cols(1)
      val machineType = cols(2).toInt
      val model = cols(3)
      val note = if (cols.length == 5) Some(cols(4)) else None

      MachineInfo(ip, machineID, machineType, model, note)
    }
  }

  /**
   *  機台編號與機台所在樓層/區域的對照表
   */
  val areaMapping: Map[String, AreaInfo] = {
    var mapping: Map[String, AreaInfo] = Map.empty
    val resourceStream = getClass.getResource(csvPrefix + "AreaInfo.csv").openStream()
    val csvFile = Source.fromInputStream(resourceStream)

    csvFile.getLines.foreach { line =>
      val Array(machineID, floor, area) = line.split("\\|")
      mapping += (machineID -> AreaInfo(floor.toInt, area))
    }

    mapping
  }

  /**
   *  機台編號到機台型號的對照表
   */
  val machineModel: Map[String, String] = machineList.map(machineInfo => machineInfo.machineID -> machineInfo.model).toMap

  /**
   *  機台編號到機台製程編號的對照表
   */
  val machineTypeID = machineList.map(machineInfo => machineInfo.machineID -> machineInfo.machineType).toMap

  /**
   *  根據機台編號取得產品 φ 別
   *
   *  此為尚未實裝條碼系統時所使用，若已確認所有條碼裝已實裝，應將此
   *  對函式刪除。
   *
   *  @param    machineID   機台編號
   *  @return               產品φ別
   */ 
  def getProduct(machineID: String) = productMapping.get(machineID).getOrElse("Unknown")

  /**
   *  根據機台編號取得機台的型號
   *
   *  @param    machineID     機台編號
   *  @return                 機台型號
   */
  def getModel(machineID: String) = machineModel.get(machineID).getOrElse("Unknown")

  /**
   *  根據機台編號取得製程 ID
   *
   *  @param    machineID   機台編號
   *  @return               機台製程編號
   */
  def getMachineTypeID(machineID: String) = machineTypeID.get(machineID).getOrElse(-1)

  /**
   *  根據機台編號取得製程名稱
   *
   *  @param    machineID   機台編號
   *  @return               機台的製程名稱
   */
  def getMachineType(machineID: String) = machineTypeName.get(getMachineTypeID(machineID)).getOrElse("Unknown")

  /**
   *  根據機台編號取得機台所在樓層
   *  
   *  @param    machineID   機台編號
   *  @return               機台所在樓層
   */
  def getMachineFloor(machineID: String): Int = areaMapping.get(machineID).map(_.floor).getOrElse(-1)

  /**
   *  根據機台編號取得機台所在區域
   *  
   *  @param    machineID   機台編號
   *  @return               機台所在區域
   */
  def getMachineArea(machineID: String): String = areaMapping.get(machineID).map(_.area).getOrElse("Unknown")

}
