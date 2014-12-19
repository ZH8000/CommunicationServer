package tw.com.zhenhai.util

import org.slf4j.Logger

object KeepRetry {

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
