package tw.com.zhenhai.util

import org.slf4j.Logger

object KeepRetry {

  /**
   *  這個函式會試著執行 block 內的程式碼，若 block 程式碼發出 Exception，
   *  則重試 waitInSeconds 秒後再試一次，重試時若仍發出 Exception，則再試
   *  第三次，依此類推。
   *
   *  @param    waitInSeconds     每次重試
   *  @param    block             要執行的程式碼
   *  @param    logger            記錄器物件
   */
  def apply(waitInSeconds: Int)(block: => Any)(implicit logger: Logger) {
    try {
      block
    } catch {
      case e: Exception =>
        logger.error("Error encountered, wait 1 second to retry...", e)
        Thread.sleep(waitInSeconds * 1000)
        apply(waitInSeconds)(block)
    }
  }

}
