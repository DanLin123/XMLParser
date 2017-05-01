name := "XMLParse"

version := "1.0"

scalaVersion := "2.11.6"


resolvers ++= Seq(
  "anormcypher" at "http://repo.anormcypher.org/",
  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)


libraryDependencies ++= Seq(
  "org.anormcypher" %% "anormcypher" % "0.9.1",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.5",
  "au.com.bytecode" % "opencsv" % "2.4"
)

        