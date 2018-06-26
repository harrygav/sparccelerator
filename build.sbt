enablePlugins(JniNative)

name := "Sparccelerator"

version := "1.0"

scalaVersion := "2.11.8"

crossPaths := false

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" excludeAll(
  ExclusionRule("io.netty", "netty"),
  ExclusionRule("io.netty", "netty-all")
)

libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-memory" % "0.8.0",
  "org.apache.arrow" % "arrow-vector" % "0.8.0"
)
target in javah := sourceDirectory.value / "native" / "include"