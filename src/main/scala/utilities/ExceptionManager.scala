/**
 * selmi sameh
 */
package org.imdb.app
package utilities

object ExceptionManager {
  def logExceptionMessage(className : String = "" , e: Exception,methodName : String): Unit = {
    //centralize all exceptions , can be used to debug & to log messages to a log file
    println(s"Exception in  : ${methodName} " + e.getMessage)
  }

}
