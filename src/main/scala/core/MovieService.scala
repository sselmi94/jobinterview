/**
 * selmi sameh
 */
package org.imdb.app
package core

import core.MovieService.{Principal, TvSeries}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source}
import org.imdb.app.utilities.{DBManager, ExceptionManager}

object MovieService extends App {

  final case class Principal(
                              id:String,
                              name: String,
                              birthYear: Int,
                              deathYear: Option[Int],
                              profession: List[String])
  final case class TvSeries(
                             original: String,
                             startYear: Int,
                             endYear: Option[Int],
                             genres: List[String])


  final case class Episode(
                            titleIdentifier: String,
                            parentTitleIdentifier: String,
                            seasonNum: Option[Int],
                            episodeNum: Option[Int]
                          )
  case class Title(
                    id: String,
                    titleType: String,
                    primaryTitle: String,
                    originalTitle: String,
                    isAdult: Boolean,
                    startYear: Option[Short],
                    endYear: Option[Short],
                    genres: Vector[String]
                  )
  case class Work(
                    tconst: String,
                    ordering: Int,
                    nconst: String,
                    category: String,
                    job: Option[String],
                    characters: Vector[String]
                  )
  try {

  implicit val system: ActorSystem = ActorSystem("IMDB")
    var impl = new MovieServiceImplementation()
    DBManager.createSchema
    //step 1 Fill database
    impl.insertPersons.runWith(Sink.last)
    impl.insertTitles.runWith(Sink.last)
    impl.insertWorked.runWith(Sink.last)
    impl.insertEpisodes.runWith(Sink.last)
    //
    //now you can run queries
   // impl.tvSeriesWithGreatestNumberOfEpisodes()
    //impl.principalsForMovieName("Titanic")

  } catch {
    case e : Exception => ExceptionManager.logExceptionMessage(this.getClass.getName,e,"main")
 }





}
trait MovieService {
  def principalsForMovieName(name: String): Source[Principal, _]
  def tvSeriesWithGreatestNumberOfEpisodes():Source[TvSeries, _]
}

object mat
