name := "meetingcheck"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",

  "org.apache.spark" %% "spark-sql" % "2.2.0",

  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)