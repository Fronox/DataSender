name := "DataSender"

version := "0.1"

scalaVersion := "2.12.8"

assemblyJarName in assembly := "task.jar"
mainClass in assembly := Some("Run.Main")

/*enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)
mainClass in Compile := Some("Run.Main")*/

libraryDependencies ++= Seq(
  "com.github.tototoshi" %% "scala-csv" % "1.3.5",
  "com.typesafe.akka" %% "akka-actor" % "2.5.21",
  "com.enragedginger" %% "akka-quartz-scheduler" % "1.8.0-akka-2.5.x",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "org.apache.kafka" %% "kafka" % "2.2.0"
)

assemblyMergeStrategy in assembly := MergeStrategy.defaultMergeStrategy

//unmanagedResourceDirectories in assembly += {baseDirectory.value / "src/main/resources"}