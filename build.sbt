import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging.autoImport._

name := "parquet-viewer"

version := "0.1"

scalaVersion := "2.11.10"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
//  "org.slf4j" % "slf4j-api" % "1.7.5",
//  "org.slf4j" % "slf4j-log4j12" % "1.7.5",

  "org.apache.parquet" % "parquet-common" % "1.8.1",
  "org.apache.parquet" % "parquet-encoding" % "1.8.1",
  "org.apache.parquet" % "parquet-column" % "1.8.1",
  "org.apache.parquet" % "parquet-hadoop" % "1.8.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1" ,
  "org.apache.hadoop" % "hadoop-common" % "2.7.1" ,
  "com.github.scopt" %% "scopt" % "3.7.0"
)


enablePlugins(JavaAppPackaging)
enablePlugins(UniversalPlugin)