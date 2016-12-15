package io.arabesque.utils 

import org.apache.log4j.{Level, LogManager}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Logging utility used primary in scala classes.
 */
trait Logging {
  protected def logName = this.getClass.getSimpleName

  protected def log = LoggerFactory.getLogger (logName)

  /** client functions are called by name in order to avoid unecessary string
   *  building **/

  protected def logInfo(msg: => String): Unit = if (log.isInfoEnabled) {
    log.info (msg)
  }

  protected def logWarning(msg: => String): Unit = if (log.isWarnEnabled) {
    log.warn (msg)
  }

  protected def logDebug(msg: => String): Unit = if (log.isDebugEnabled) {
    log.debug (msg)
  }
  
  protected def logError(msg: => String): Unit = if (log.isErrorEnabled) {
    log.error (msg)
  }

  /** **/

  protected def setLogLevel(level: String): Unit = {
    LogManager.getLogger(logName).setLevel(Level.toLevel(level))
  }

}
