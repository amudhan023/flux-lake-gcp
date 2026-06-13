name := "pipeline-scala"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"
val deltaVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"     % sparkVersion % "provided",
  "io.delta"         %% "delta-spark"   % deltaVersion % "provided",
  "org.scalatest"    %% "scalatest"     % "3.2.17"     % Test,
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-unused",
)
