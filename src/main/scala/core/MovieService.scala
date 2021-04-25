package org.imdb.app
package core

import core.MovieService.{Principal, TvSeries}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.Status.{Failure, Success}
import akka.stream.{ClosedShape, Materializer}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.util.ByteString
import org.imdb.app.utilities.{ApplicationConstants, ExceptionManager}

import java.nio.file.Paths
import scala.Option
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.math.Numeric.BigDecimalAsIfIntegral.mkNumericOps
import scala.util.Try
import scala.collection.mutable.Map
object MovieService extends App {

  final case class Principal(
                              name: String,
                              birthYear: Int,
                              deathYear: Option[Int],
                              profession: List[String])
  final case class TvSeries(
                             original: String,
                             startYear: Int,
                             endYear: Option[Int],
                             genres: List[String])



  try {

  implicit val system: ActorSystem = ActorSystem("IMDB")

    var implementation = new MovieServiceImplementation
    implementation.principalsForMovieName("Bohemios").runForeach(println)



  } catch {
   case e: Exception => ExceptionManager.logExceptionMessage(e,"Main Method")
 }





}
trait MovieService {
  def principalsForMovieName(name: String): Source[Principal, _]
  def tvSeriesWithGreatestNumberOfEpisodes():Source[TvSeries, _]
}
