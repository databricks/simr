import AssemblyKeys._ // put this at the top of the file

assemblySettings

organization := "spark"

name := "simr.Simr"

version := "1.0-SNAPSHOT"

scalaVersion := "2.9.3"

mainClass in assembly := Some("simr.Simr")
