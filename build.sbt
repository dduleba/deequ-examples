ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "deequ-examples"
  )

libraryDependencies += "com.amazon.deequ" % "deequ" % "2.0.1-spark-3.2"