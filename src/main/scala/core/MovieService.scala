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
    //implementation.readNameFile
    implementation.principalsForMovieName("Bohemios").runForeach(println)


     // val sink1 = Sink.foreach(println)
  //  val sink2 = Sink.fold[Int, ByteString](0)((acc, _) => acc + 1)



    //implementation.readFile(ApplicationConstants.TITLE_PRINICPALS_PATH).filter(_(0).equals("tt0000502")).map(x => x.mkString(",")).runForeach(println)
   /* var idMovie = implementation.readFile(ApplicationConstants.NAME_DATASET_PATH).map(line => line(0).toString)
      //.filter(line => line(2).
       // equals("Bohemios") && line(1).equals("movie")).map(line => line(0).toString)
    // step 2 read
    var result : Future[List[String]] = idMovie.runWith(Sink.collection[String,List[String]])
  result.foreach(println)
*/

  } catch {
   case e: Exception => ExceptionManager.logExceptionMessage(e)
 }





}
trait MovieService {
  def principalsForMovieName(name: String): Source[Principal, _]
  def tvSeriesWithGreatestNumberOfEpisodes():Source[TvSeries, _]
}
