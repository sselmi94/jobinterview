package org.imdb.app
package core

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.JavaFlowSupport.Sink
import akka.stream.scaladsl.{FileIO, Framing, Source}
import akka.util.ByteString
import org.imdb.app.core.MovieService.{Episode, Principal, Title, TvSeries, Work}
import org.imdb.app.utilities.{ApplicationConstants, DBManager, ExceptionManager}

import java.nio.file.Paths
import scala.collection.immutable
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
class MovieServiceImplementation(implicit val system: ActorSystem) extends MovieService  {

/*
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
  }*/

  def principalsForMovieNameQuery(name: String) ={

    DBManager.getMovies(name)
  }

  def titlesWithLargestEpisodeCount(limit: Int) = {
    DBManager.getTitleWithLargestEpisode(limit)

  }

  def principalsForMovieName(name: String): Source[Principal, NotUsed] =
    Source
      .future(
        Future {
          principalsForMovieNameQuery(name)
        }
      )
      .flatMapConcat(Source(_))

/*
  def tvSeriesWithGreatestNumberOfEpisodes(limit: Int): Source[TitleWithEpisodeCount, NotUsed] =
      Source.future(
        titlesWithLargestEpisodeCount(limit)
      ).flatMapConcat(Source(_)))
}*/
  def mapToPrincipal(splited: Array[String] ): Option[Principal] = {
    //split professions
    var professions = splited(4).split(",").toList
    var birthYear = if (splited(2) == "\\N") -1 else splited(2).toInt

    var deathYear = if (splited(3) == "\\N") None else Some(splited(3).toInt)
    //
    Some(Principal(splited(0),splited(1),birthYear,deathYear ,professions))
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

  def mapToTitle(splited: Array[String] ): Option[Title] = {
    //split professions
    var genres = splited(8).split(",").toVector
    var startYear : Option[Short] = if (splited(5) == "\\N") None else Some(splited(5).toShort)

    var endYear = if (splited(6) == "\\N") None else Some(splited(6).toShort)

    var originalTitle = splited(3)
    //
    Some(Title(splited(0),splited(1),splited(2),originalTitle,splited(4).toBoolean,startYear,endYear,genres))
  }
  def mapToEpisode(splited: Array[String] ): Option[Episode] = {
    var season : Option[Int] = if (splited(2) == "\\N") None else Some(splited(2).toInt)
    var epNum : Option[Int] = if (splited(3) == "\\N") None else Some(splited(3).toInt)


    //
    Some(Episode(splited(0),splited(1),season,epNum))
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
      case e: Exception => { ExceptionManager.logExceptionMessage(this.getClass.getName,e,"firstLine")
      None}
    }
    finally {
      src.close()
    }
  }

  def insertTitle(title: Option[Title]) = {
    var sql = "INSERT OR IGNORE INTO titles VALUES(?,?,?,?,?,?,?,?)"
    DBManager.executeStatement(sql,title.get)
  }
  def insertPerson(person: Option[Principal]) = {
    var sql = s"INSERT INTO persons VALUES(?,?,?,?,?)"

    DBManager.executeStatement(sql,person.get)
  }
  def insertWork(work: Option[Work]) = {
    var sql = s"INSERT INTO worked VALUES(?,?,?,?,?,?)"

    DBManager.executeStatement(sql,work.get)
  }
  def insertEpisode(episode: Option[Episode]) = {
    var sql = s"INSERT INTO episodes VALUES(?,?,?,?)"

    DBManager.executeStatement(sql,episode.get)
  }
  //database
  def insertTitles = {
    println("inserting titles")
    readFile(ApplicationConstants.TITLE_BASIC_PATH)
      .map(mapToTitle(_)).runForeach(item => insertTitle(item))
  }

  def insertPersons = {
    println("inserting persons")
    readFile(ApplicationConstants.NAME_DATASET_PATH).map(mapToPrincipal(_)).runForeach(item => insertPerson(item))

  }

  def mapToWork(splited: Array[String]) = {
    var characters = splited(5).split(",").toVector
    var category = splited(3)
    var job : Option[String] = if (splited(4) == "\\N") None else Some(splited(4).toString)


    //
    Some(Work(splited(0),splited(1).toInt,splited(2),category,job,characters))
  }

  def insertWorked = {
    readFile(ApplicationConstants.TITLE_PRINICPALS_PATH)
      .map(mapToWork(_)).runForeach(item => insertWork(item))
  }

  def insertEpisodes = {
    readFile(ApplicationConstants.TITLE_EPISODE_PATH)
      .map(mapToEpisode(_)).runForeach(item => insertEpisode(item))
  }
/*
  def insertPeople(personRepository: PersonRepository[IO]): Source[Unit, _] =
    RawSources.peopleSource
      .grouped(Config.insertionChunkSize)
      .mapAsyncUnordered(Config.insertionParallelism)(
        personRepository.insertPeople(_).unsafeToFuture()
      )

  def insertPrincipals(principalRepository: PrincipalRepository[IO]): Source[Unit, _] =
    RawSources.principalsSource
      .grouped(Config.insertionChunkSize)
      .mapAsyncUnordered(Config.insertionParallelism)(
        principalRepository.insertPrincipals(_).unsafeToFuture()
      )

  def insertEpisodes(episodeRepository: EpisodeRepository[IO]): Source[Unit, _] =
    RawSources.episodesSource
      .grouped(Config.insertionChunkSize)
      .mapAsyncUnordered(Config.insertionParallelism)(
        episodeRepository.insertEpisodes(_).unsafeToFuture()
      )

*/
}
