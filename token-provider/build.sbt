name := "LocalSparkJars"
version := "0.3"
scalaVersion := "2.12.18"
// One Scala line per Fabric runtime profile; `sbt +package` builds both:
//   2.12 -> fabric-1.3 (Spark 3.5 / Delta 3.2 / Hadoop 3.3)
//   2.13 -> fabric-2.0 (Spark 4.1 / Delta 4.2 / Hadoop 3.4)
// Keep these in step with src/local_spark_mcp/profiles.py.
crossScalaVersions := Seq("2.12.18", "2.13.17")

// Everything here is "provided": hadoop-azure, spark-sql, and delta-spark are
// all on Spark's classpath at runtime (the latter two via the Delta/Hadoop
// package mechanism). We ship only our classes as a thin jar via `sbt package`
// and reference it with spark.jars. The jar is NOT binary-compatible across
// Spark/Delta minors, hence one jar per profile.
libraryDependencies ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 13)) => Seq(
      "org.apache.hadoop" % "hadoop-azure"  % "3.4.1" % "provided",
      "org.apache.hadoop" % "hadoop-common" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-sql"     % "4.1.1" % "provided",
      "io.delta"         %% "delta-spark"   % "4.2.0" % "provided",
    )
    case _ => Seq(
      "org.apache.hadoop" % "hadoop-azure"  % "3.3.4" % "provided",
      "org.apache.hadoop" % "hadoop-common" % "3.3.4" % "provided",
      "org.apache.spark" %% "spark-sql"     % "3.5.0" % "provided",
      "io.delta"         %% "delta-spark"   % "3.2.0" % "provided",
    )
  }
}
