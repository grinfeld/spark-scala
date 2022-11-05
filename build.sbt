name := "spark-scala"

version := "0.1"

scalaVersion := "2.12.14"

scalacOptions += "-Ypartial-unification"

val versions = new {
  val logback = "1.2.3"
  val scala_logging = "3.9.2"
  val cats = "2.4.2"
  val mysql = "8.0.23"
  val spark = "3.2.0"
  val typesafe_config = "1.4.1"
  val aws_sdk_s3 = "1.11.655"
  val guava = "23.0"
  val hadoop_aws = "3.2.2"
  val lettuce = "6.1.5.RELEASE"
  val iceberg = "0.14.1"
  val dyschema = "5.0.4"
  val tests = new {
    val scalaTest = "3.2.0"
    val mockito = "1.16.0"
  }
}

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala",
    compileOrder:= CompileOrder.JavaThenScala,
    resolvers += Resolver.mavenLocal,
    resolvers += Resolver.jcenterRepo,
    resolvers += "event-collection-common" at "https://dy1.jfrog.io/artifactory/event-collection-common",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % versions.typesafe_config,
      "com.typesafe.scala-logging" %% "scala-logging" % versions.scala_logging,
      "org.typelevel" %% "cats-core" % versions.cats,
      "ch.qos.logback" % "logback-classic" % versions.logback,
      "mysql" % "mysql-connector-java" % versions.mysql,
      "io.lettuce" % "lettuce-core" % versions.lettuce,
      "com.google.guava" % "guava" % versions.guava,
      "com.amazonaws" % "aws-java-sdk" % versions.aws_sdk_s3,
      "org.apache.hadoop" % "hadoop-aws" % versions.hadoop_aws,
      "org.apache.spark" %% "spark-core" % versions.spark,
      "org.apache.spark" %% "spark-sql" % versions.spark,
      "org.apache.spark" %% "spark-avro" % versions.spark,
      "org.apache.spark" %% "spark-hive" % versions.spark,
      "com.dynamicyield" % "event-collection-schema" % versions.dyschema,
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.2" % versions.iceberg,
      "org.scalatest" %% "scalatest" % versions.tests.scalaTest % "test",
      "org.mockito" %% "mockito-scala-scalatest" % versions.tests.mockito % "test"
    )
  )