import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging.autoImport._

name := "pq"

organization := "com.art4ul"

version := "0.1"

scalaVersion := "2.11.10"

val log4jVersion = "2.11.0"
val parquetVersion = "1.8.2"
val hadoopVersion = "2.7.1"
val slf4jVersion = "1.7.25"

dependencyOverrides += "org.slf4j" % "slf4j-api" % slf4jVersion

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.3.0",
  "org.slf4j" % "log4j-over-slf4j" % slf4jVersion,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.antlr" % "antlr4" % "4.7.1",
  "org.antlr" % "antlr4-runtime" % "4.7.1",

  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "de.vandermeer" % "asciitable" % "0.3.2",
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.apache.parquet" % "parquet-common" % parquetVersion,
  "org.apache.parquet" % "parquet-encoding" % parquetVersion,
  "org.apache.parquet" % "parquet-column" % parquetVersion,
  "org.apache.parquet" % "parquet-hadoop" % parquetVersion,
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion
    exclude("log4j" ,"log4j"),
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion
    exclude("log4j" ,"log4j")
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

enablePlugins(JavaAppPackaging)
enablePlugins(UniversalPlugin)

antlr4Settings
antlr4PackageName in Antlr4 := Some("com.art4ul.antlr")
antlr4Version in Antlr4 := "4.7.1"