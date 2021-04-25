package org.imdb.app
package core

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString
import org.imdb.app.core.MovieService.{Principal, TvSeries}
import org.imdb.app.utilities.{ApplicationConstants, ExceptionManager}

import java.nio.file.Paths
import scala.collection.mutable.Map
class MovieServiceImplementation(implicit val system: ActorSystem) extends MovieService  {


  override def principalsForMovieName(name: String): Source[MovieService.Principal, _] = {
     readFile(ApplicationConstants.TITLE_BASIC_PATH)
      .filter(line => line(2).
      equals(name) && line(1).equals("movie")).map(line => line(0).toString).flatMapConcat(idOfMovie => lookupPrincipal(idOfMovie)).map(p => p.get)

  }


  override def tvSeriesWithGreatestNumberOfEpisodes(): Source[MovieService.TvSeries, _] = {
    val sourceEpisode = readFile(ApplicationConstants.TITLE_EPISODE_PATH)
    readFile(ApplicationConstants.TITLE_BASIC_PATH).filter(line => line(1) == "tvSeries").flatMapConcat(
      tvserie => {
        sourceEpisode.filter(item => item(1).equals(tvserie(0))).map(line => tvserie)
      }
    ) .map(x =>
    {
      (x(2),1)})
      .fold(Map[String, Int]().empty) { (map, item) =>{
        map.updated(item._1, map.getOrElse(item._1, 0) + 1)
      }}
    null
  }
  def lookupPrincipal(idOfMov: String) = {
    readFile(ApplicationConstants.TITLE_PRINICPALS_PATH).filter(item => item(0).equals(idOfMov) && !item(3).equals("actor") && ! (item(3).equals("actress")))
      .map(_(2).toString).flatMapConcat(idPerson => {
      readFile(ApplicationConstants.NAME_DATASET_PATH).filter(_(0).equals(idPerson)).map(mapToPrincipal(_))
    })
  }
  def mapToPrincipal(splited: Array[String] ): Option[Principal] = {
    //split professions
    var professions = splited(4).split(",").toList
    var birthYear = if (splited(2) == "\\N") -1 else splited(2).toInt

    var deathYear = if (splited(3) == "\\N") None else Some(splited(3).toInt)
    //
    Some(Principal(splited(1),birthYear,deathYear ,professions))
  }
  def mapToTvSeries(splited: Array[String] ): Option[TvSeries] = {
    //split professions
    var genres = splited(8).split(",").toList
    var startYear : Option[Int] = if (splited(5) == "\\N") None else Some(splited(5).toInt)

    var endYear = if (splited(6) == "\\N") None else Some(splited(6).toInt)
    var originalTitle = splited(3)
    //
    Some(TvSeries(originalTitle,startYear.getOrElse(0),endYear ,genres))
  }


  def readFile(pathOfFile : String) = {

    FileIO.fromPath(Paths.get(pathOfFile),chunkSize = 4092)
        .via(Framing.delimiter(ByteString("\n"), Int.MaxValue,true).map(_.utf8String))

      .filter(_ != firstLine(pathOfFile).getOrElse(""))
      .map(_.split("\t"))

  }
  def firstLine(path : String): Option[String] = {
    val src = io.Source.fromFile(path)
    try {
      src.getLines.find(_ => true)
    }
    catch {
      case e: Exception => { ExceptionManager.logExceptionMessage(e,"firstLine")
      None}
    }
    finally {
      src.close()
    }
  }
}
