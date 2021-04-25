name := "imdb-application"

version := "0.1"

scalaVersion := "2.12.11"

idePackagePrefix := Some("org.imdb.app")
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-stream
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.6.13"
// https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.34.0"
