name := "LocalSparkJars"
version := "0.2"
scalaVersion := "2.12.18"

// Everything here is "provided": hadoop-azure, spark-sql, and delta-spark are
// all on Spark's classpath at runtime (the latter two via the Delta/Hadoop
// package mechanism). We ship only our classes as a thin jar via `sbt package`
// and reference it with spark.jars.
libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-azure"      % "3.3.4" % "provided",
  "org.apache.hadoop" % "hadoop-common"     % "3.3.4" % "provided",
  "org.apache.spark" %% "spark-sql"         % "3.5.0" % "provided",
  "io.delta"         %% "delta-spark"       % "3.2.0" % "provided",
)
