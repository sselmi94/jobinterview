package org.imdb.app
package utilities

object ExceptionManager {
  def logExceptionMessage(e: Exception,methodName : String): Unit = {
    println(s"Exception in  : ${methodName} " + e.getMessage)
  }

}
