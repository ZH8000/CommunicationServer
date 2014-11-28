package tw.com.zhenhai.main

import tw.com.zhenhai.model._
import tw.com.zhenhai.db._

import com.mongodb.casbah.Imports._
import org.slf4j.LoggerFactory

object DailyImport {

  val mongoClient = MongoClient("localhost")
  val mongoDB = mongoClient("zhenhaiDaily")
  val mongoProcessor = new MongoProcessor(mongoClient)

  def main(args: Array[String]) = {
    println(args(0))
    val collection = mongoDB(args(0))
    var counter = 0
    val totalCount = collection.count()

    collection.find.foreach { doc =>
      val record = Record(doc)
      println(s"Processing record [$counter / $totalCount] ....")

      record.countQty match {
        case -1 => mongoProcessor.addMachineAlert(record)
        case  n => mongoProcessor.addRecord(record)
      }

      counter += 1
    }
  }
}
