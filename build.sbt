import sbt.addCompilerPlugin

lazy val app = (project in file(".")).settings(
  name := "data-preparation",
  version := "0.1",
  scalaVersion := "2.13.6",
  libraryDependencies ++=Dependencies(),
  assemblyJarName := "data-preparation.jar",
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")
)