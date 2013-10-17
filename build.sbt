import AssemblyKeys._ // put this at the top of the file

assemblySettings

organization := "spark"

name := "org.apache.spark.simr.SimrJob"

version := "1.0-SNAPSHOT"

scalaVersion := "2.9.3"

mainClass in assembly := Some("org.apache.spark.simr.SimrJob")

javacOptions ++= Seq("-source", "1.6", "-target", "1.6")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", xs @ _*)         => MergeStrategy.discard
    case PathList("org", "fusesource", xs @ _*)         => MergeStrategy.first
    case x => old(x)
  }
}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
  cp filter {_.data.getName == "spark.jar"}
}

jarName in assembly := "simr.jar"
