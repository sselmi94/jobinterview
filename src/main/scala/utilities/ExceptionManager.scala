package org.imdb.app
package utilities

object ExceptionManager {
  def logExceptionMessage(className : String = "" , e: Exception,methodName : String): Unit = {
    println(s"Exception in  : ${methodName} " + e.getMessage)
  }

}
