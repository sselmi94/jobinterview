package org.imdb.app
package utilities

object ExceptionManager {
  def logExceptionMessage(e: Exception): Unit = {
    //write exception message to log
    println("Exception Manager : " + e.getMessage)
  }

}
