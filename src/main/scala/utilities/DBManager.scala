/**
 * selmi sameh
 */

package org.imdb.app
package utilities

import org.imdb.app.core.MovieService.{Episode, Principal, Title, TvSeries, Work}

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}


object DBManager{
  def getTitleWithLargestEpisode(limit: Int) : Vector[TvSeries] = {
    try {


      if (database == null) doConnection()

      if (database != null) {
        var sql = "SELECT t.*, COUNT(1) as episode_count FROM episodes e JOIN titles t ON t.id = parent_title_id GROUP BY e.parent_title_id ORDER BY episode_count DESC LIMIT ?"
        var stmt = database.prepareStatement(sql)
        stmt.setInt(1, limit)
        var rs = stmt.executeQuery()
        var seq: Vector[TvSeries] = Vector[TvSeries]()
        while (rs.next()) {
          val original = rs.getString("original_title")
          val startyear = rs.getInt("start_year")
          val endYear = Option(rs.getInt("end_year"))
          var tv = TvSeries(original, startyear, endYear, null)
          seq :+ tv
        }
        seq
      }
      Vector.empty
    } catch {
    case e : Exception => {
      ExceptionManager.logExceptionMessage("", e, "")
      Vector.empty
    }
  }

  }

  def getPrinicpals(name: String) : Vector[Principal] =  {
    try{
    if(database == null)doConnection()
    if(database != null){
      var sql =  "SELECT persons.id,persons.primary_name,persons.birth_year,persons.death_year,persons.primary_profession FROM titles t INNER JOIN worked ON worked.tconst = t.id INNER JOIN persons on worked.nconst = persons.id WHERE (t.original_title = $movieName OR t.primary_title = ?) AND t.title_type = 'movie'"
      var stmt = database.prepareStatement(sql)
      stmt.setString(1,name)
      var rs = stmt.executeQuery()

      var seq : Vector[Principal] = Vector[Principal]()
      while (rs.next()) {
        val id = rs.getString("persons.id")
        val primary = rs.getString("persons.primary_name")
        val birthyear = rs.getInt("persons.birth_year")
        val death =  Option(rs.getInt("persons.death_year"))
        var prin = Principal(id,primary,birthyear,death,null)
        seq :+ prin

      }
      seq
    }
      Vector.empty
    }catch {
      case e : Exception => {
        ExceptionManager.logExceptionMessage("", e, "")
        Vector.empty
      }
    }
  }

  def createSchema = {
    try{

    if(database != null){
    var stmt = database.createStatement()
    stmt.execute(sqlSchemaPrincipal)
    stmt.execute(sqlSchemaEpisode)
    stmt.execute(sqlSchemaPrincipalInTitle)
    stmt.execute(sqlSchemaTitle)}}
    catch {
      case e : Exception =>  {
        ExceptionManager.logExceptionMessage(this.getClass.getName,e,"executeStatemet")}}
  }

  def executeStatement(sql: String,instance : AnyRef) : AnyRef = {
    var stmt : PreparedStatement = null
    try {
      if(database == null) doConnection()
         stmt = database.prepareStatement(sql)
        instance match {
          case  Principal(_,_,_,_,_) => {
            var person = instance.asInstanceOf[Principal]
            stmt.setString(1,person.id )
            stmt.setString(2,person.name )
            stmt.setInt(3,person.birthYear )
            stmt.setInt(4,person.deathYear.getOrElse(0) )
            stmt.setString(5,person.profession.mkString(",") )

          }
          case Title(_,_,_,_,_,_,_,_) => {

            var title = instance.asInstanceOf[Title]
            stmt.setString(1,title.id )
            stmt.setString(2,title.titleType )
            stmt.setString(3,title.primaryTitle )
            stmt.setString(4,title.originalTitle )
            stmt.setBoolean(5,title.isAdult )
            stmt.setShort(6,title.startYear.getOrElse(0) )
            stmt.setShort(7,title.endYear.getOrElse(0) )
            stmt.setString(8,title.genres.mkString(","))




          }

          case  Work(_,_,_,_,_,_) => {
            var work = instance.asInstanceOf[Work]
            stmt.setString(1,work.tconst )
            stmt.setInt(2,work.ordering )
            stmt.setString(3,work.nconst )
            stmt.setString(4,work.category )
            stmt.setString(5,work.job.getOrElse("") )

          }

          case  Episode(_,_,_,_) => {
            var episode: Episode = instance.asInstanceOf[Episode]
            stmt.setString(1,episode.titleIdentifier )
            stmt.setString(2,episode.parentTitleIdentifier )
            stmt.setInt(3,episode.seasonNum.getOrElse(0) )
            stmt.setInt(4,episode.episodeNum.getOrElse(0) )


          }

        }
       stmt.executeUpdate()
         instance

    }catch {
      case e : Exception =>  {
        ExceptionManager.logExceptionMessage(this.getClass.getName,e,"executeStatemet")
      None

      }
    }finally {
      stmt.closeOnCompletion()
    }
  }
  var database : Connection = null
  final var sqlSchemaPrincipal = """
    CREATE TABLE IF NOT EXISTS persons(
      id VARCHAR(12) NOT NULL,
      primary_name VARCHAR(256) NOT NULL,
      birth_year TINYINT,
      death_year TINYINT,
      primary_profession TEXT,
      PRIMARY KEY (id)
    )
    """
  final var sqlSchemaTitle = """CREATE TABLE IF NOT EXISTS titles(
        id VARCHAR(12) NOT NULL,
        title_type VARCHAR(20) NOT NULL,
        primary_title VARCHAR(256) NOT NULL,
        original_title VARCHAR(256) NOT NULL,
        is_adult BOOLEAN NOT NULL,
        start_year TINYINT,
        end_year TINYINT,
        genres TEXT,
        PRIMARY KEY (id)
      )"""

  final var sqlSchemaEpisode = """
    CREATE TABLE IF NOT EXISTS episodes(
      title_id VARCHAR(12) NOT NULL,
      parent_title_id VARCHAR(12) NOT NULL,
      season_number INT,
      episode_number INT,
      FOREIGN KEY(title_id) REFERENCES titles(id),
      FOREIGN KEY(parent_title_id) REFERENCES titles(id),
      PRIMARY KEY (title_id)
    )
    """
  final var sqlSchemaPrincipalInTitle =
    """
    CREATE TABLE IF NOT EXISTS worked(
      tconst  VARCHAR(12) NOT NULL,
      nconst VARCHAR(12) NOT NULL,
      ordering INT,
      category VARCHAR(256),
      job VARCHAR(256),
      characters TEXT,
      FOREIGN KEY(tconst) REFERENCES titles(id),
      FOREIGN KEY(nconst) REFERENCES persons(id),
      PRIMARY KEY (tconst,nconst)
    )
    """
  def doConnection() = {
    try {
      database =
        DriverManager.getConnection("jdbc:sqlite:sample.db")
      createSchema

    } catch {
      case e : Exception => ExceptionManager.logExceptionMessage(this.getClass.getName,e,"doIngestionInSQLITE")
    }
  }
  def doIngestionInSQLITE(): Unit ={
    try {
      database =
        DriverManager.getConnection("jdbc:sqlite:test.db")

    } catch {
      case e : Exception => ExceptionManager.logExceptionMessage(this.getClass.getName,e,"doIngestionInSQLITE")
  }
  }
  }
