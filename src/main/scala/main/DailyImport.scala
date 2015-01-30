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
    val collection = mongoDB("2015-01-31")
    var counter = 0
    val totalCount = collection.count()

    collection.find.foreach { doc =>

      val record = Record(doc)
      println(s"Processing record [$counter / $totalCount] ....")

      record.countQty match {
        case -1 => mongoProcessor.addMachineAlert(record, isImportFromDaily = true)
        case  n => mongoProcessor.addRecord(record, isImportFromDaily = true)
      }

      counter += 1
    }
  }
}
