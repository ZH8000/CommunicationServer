package tw.com.zhenhai.main

import tw.com.zhenhai.model._
import tw.com.zhenhai.db._

import com.mongodb.casbah.Imports._
import org.slf4j.LoggerFactory
import scala.util.Random

object DailyImport {

  val mongoClient = MongoClient("localhost")
  val mongoDB = mongoClient("zhenhaiDaily")
  val mongoProcessor = new MongoProcessor(mongoClient)

  def main(args: Array[String]) = {
    val collection = mongoDB(args(0))
    var counter = 0
    val totalCount = collection.count()

    collection.find.foreach { doc =>

      val record = Record(doc)
      val fixedRecord = record.product match {
        case "Unknow" => record.copy(product = "Unknown")
        case _        => record
      }

      println(s"Processing record [$counter / $totalCount] ....")

      fixedRecord.countQty match {
        case -1 => mongoProcessor.addMachineAlert(fixedRecord, isImportFromDaily = true)
        case  n => mongoProcessor.addRecord(fixedRecord, isImportFromDaily = true)
      }

      counter += 1
    }
  }
}
