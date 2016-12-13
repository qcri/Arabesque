package io.arabesque.utils 

// log
import org.apache.log4j.{Level, Logger}

trait Logging {
  protected def logName = this.getClass.getSimpleName

  protected def log = Logger.getLogger (logName)

  protected def logInfo(msg: String): Unit = {
    log.info (msg)
  }

  protected def logWarning(msg: String): Unit = {
    log.warn (msg)
  }

  protected def logDebug(msg: String): Unit = {
    log.debug (msg)
  }
  
  protected def logError(msg: String): Unit = {
    log.error (msg)
  }
}
