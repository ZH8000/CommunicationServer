package tw.com.zhenhai.db

import tw.com.zhenhai.model._

trait OrderedDBProcessor {
  def updateDB(record: Record): Unit
}


