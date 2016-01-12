
lazy val commonSettings = Seq(
  organization := "com.example",
  version := "0.1.0",
  scalaVersion := "2.11.6"
  )
lazy val root = (project in file(".")).
  settings(commonSettings: _*).
    settings(
      name := "similar-users-evaluator",
      libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"
    )

