name := "HttpTokenProvider"
version := "0.1"
scalaVersion := "2.12.18"

// hadoop-azure supplies the CustomTokenProviderAdaptee interface. It is already
// on Spark's classpath at runtime, so it's "provided" — we ship only our class
// via `sbt package` (a thin jar) and reference it with spark.jars.
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.3.4" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4" % "provided"
