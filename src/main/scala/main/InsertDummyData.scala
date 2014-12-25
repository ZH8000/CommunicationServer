package tw.com.zhenhai.main

import tw.com.zhenhai.model._
import tw.com.zhenhai.db._

import com.mongodb.casbah.Imports._
import org.slf4j.LoggerFactory
import scala.util.Random

object InsertDummyData {

  class Order(mongoProcessor: MongoProcessor, randomStopMax: Int, lotNo: String, inputCount: Long, product: String, 
  	      step1: String, step2: String, step3: String, step4: String, step5: String,
  	      worker1: String, worker2: String, worker3: String, worker4: String, worker5: String) extends Thread {

    val dateFormatter = new java.text.SimpleDateFormat("yyyy-MM-dd")
    var currentTimestamp: Long = (new java.util.Date).getTime / 1000
    var step1Count: Long = 0
    var step2Count: Long = 0
    var step3Count: Long = 0
    var step4Count: Long = 0
    var step5Count: Long = 0

    def randomTimeStop = Random.nextInt(randomStopMax) + 1
    def randomCountQty = Random.nextInt(5) + 1
    def randomErrorQty = Random.nextInt(3) + 1

    override def run() {

      val stopCount = (inputCount * 1.04).toInt
      val step1DefactID = List(2, 3, 4, 5, 6, 7)
      val step2DefactID = List(20, 33, 38, 30)
      val step3DefactID = List(13, 14, 15)

      // Step1 TSW-100T
      while (step1Count < stopCount) {
        val countQty = randomCountQty
	val timeStop = randomTimeStop
	val timestamp = dateFormatter.format(new java.util.Date(currentTimestamp * 1000L))
	val shiftDate = dateFormatter.format(Record.getShiftTime(currentTimestamp))
	val record = Record(
	  "0", lotNo, stopCount, countQty, currentTimestamp, 0, 
	  "192.168.0.0", 8, step1, worker1, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	)

	println(s"add step1[$step1Count / $stopCount] $record....")
	mongoProcessor.addRecord(record)

	if (timeStop >= (randomStopMax * 0.75)) {
	  val errorCount = randomErrorQty
	  val defactID = Random.shuffle(step1DefactID).head
  	  val record = Record(
	    "0", lotNo, stopCount, 0, currentTimestamp, errorCount, 
  	    "192.168.0.0", defactID, step1, worker1, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	  )
	  mongoProcessor.addRecord(record)
	}

	currentTimestamp += timeStop
	step1Count += countQty
      }

      // Step 2
      while (step2Count < inputCount) {
        val countQty = randomCountQty
	val timeStop = randomTimeStop
	val timestamp = dateFormatter.format(new java.util.Date(currentTimestamp * 1000L))
	val shiftDate = dateFormatter.format(Record.getShiftTime(currentTimestamp))
	val record = Record(
	  "0", lotNo, stopCount, countQty, currentTimestamp, 0, 
	  "192.168.0.0", 7, step2, worker2, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	)

	println(s"add step2[$step2Count / $inputCount] $record....")
	mongoProcessor.addRecord(record)

	if (timeStop >= (randomStopMax * 0.75)) {
	  val errorCount = randomErrorQty
	  val defactID = Random.shuffle(step2DefactID).head
  	  val record = Record(
	    "0", lotNo, stopCount, 0, currentTimestamp, errorCount, 
  	    "192.168.0.0", defactID, step2, worker2, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	  )
	  mongoProcessor.addRecord(record)
	}

	currentTimestamp += timeStop
	step2Count += countQty
      }

      // Step 3
      while (step3Count < inputCount) {
        val countQty = randomCountQty
	val timeStop = randomTimeStop
	val timestamp = dateFormatter.format(new java.util.Date(currentTimestamp * 1000L))
	val shiftDate = dateFormatter.format(Record.getShiftTime(currentTimestamp))
	val record = Record(
	  "0", lotNo, stopCount, countQty, currentTimestamp, 0, 
	  "192.168.0.0", 16, step3, worker3, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	)

	println(s"add step3[$step3Count / $inputCount] $record....")
	mongoProcessor.addRecord(record)

	if (timeStop >= (randomStopMax * 0.75)) {
	  val errorCount = randomErrorQty
	  val defactID = Random.shuffle(step2DefactID).head
  	  val record = Record(
	    "0", lotNo, stopCount, 0, currentTimestamp, errorCount, 
  	    "192.168.0.0", defactID, step3, worker3, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	  )
	  mongoProcessor.addRecord(record)
	}

	currentTimestamp += timeStop
	step3Count += countQty
      }

      // Step 4
      while (step4Count < inputCount) {
        val countQty = randomCountQty
	val timeStop = randomTimeStop
	val timestamp = dateFormatter.format(new java.util.Date(currentTimestamp * 1000L))
	val shiftDate = dateFormatter.format(Record.getShiftTime(currentTimestamp))
	val record = Record(
	  "0", lotNo, stopCount, countQty, currentTimestamp, 0, 
	  "192.168.0.0", 16, step4, worker4, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	)
	println(s"add step4[$step4Count / $inputCount] $record....")

	mongoProcessor.addRecord(record)
	currentTimestamp += timeStop
	step4Count += countQty
      }

      // Step 5
      while (step5Count < inputCount) {
        val countQty = (randomCountQty * 10.253).toInt
	val timeStop = randomTimeStop
	val timestamp = dateFormatter.format(new java.util.Date(currentTimestamp * 1000L))
	val shiftDate = dateFormatter.format(Record.getShiftTime(currentTimestamp))
	val record = Record(
	  "0", lotNo, stopCount, countQty, currentTimestamp, 0, 
	  "192.168.0.0", 5, step5, worker5, "0", "0", "0", "0", product, timestamp, "aa:aa:bb:cc:dd:ee", shiftDate
	)

	println(s"add step5[$step5Count / $inputCount] $record....")

	mongoProcessor.addRecord(record)
	currentTimestamp += timeStop
	step5Count += countQty
      }

    }
  }




  def main(args: Array[String]) = {
    val mongoClient = MongoClient("localhost")
    val mongoDB = mongoClient("zhenhaiDaily")
    val mongoProcessor = new MongoProcessor(mongoClient)

    // TSW-100T
    // FTO-2200A
    // CS-210
    // YC-600B
    // NCR-236

    def randomWorkers = Random.shuffle(mongoClient("zhenhai")("worker").toList.map(x => x._id.get.toString)).head

    val order1 = new Order(
      mongoProcessor, 5, "BLX102111", (50000 * 1.04).toLong, "6x11", "E21", "G35", "A06", "A57", "T12",
      randomWorkers, randomWorkers, randomWorkers, randomWorkers, randomWorkers)
    val order2 = new Order(
      mongoProcessor, 50, "CLX102111", (50000 * 1.04).toLong, "16x25", "E33", "G47", "A21", "A67", "T16",
      randomWorkers, randomWorkers, randomWorkers, randomWorkers, randomWorkers)

    order1.start();
    order2.start();

    order1.join();
    order2.join();

  }

}


