name := "spark-scala"

version := "0.1"

scalaVersion := "2.12.14"

idePackagePrefix := Some("com.dy.spark.scala")

scalacOptions += "-Ypartial-unification"

val versions = new {
  val logback = "1.2.3"
  val scala_logging = "3.9.2"
  val cats = "2.4.2"
  val mysql = "8.0.23"
  val spark = "3.1.2"
  val typesafe_config = "1.4.1"
  val coll_schema = "3.1.7"
  val dy_metrics = "0.1.2"
  val aws_sdk_s3 = "1.11.655"
  val hadoop_aws = "3.2.0"
  val tests = new {
    val scalaTest = "3.2.0"
    val mockito = "1.16.0"
  }
}

lazy val root = (project in file("."))
  .settings(
    name := "citizens",
    compileOrder:= CompileOrder.JavaThenScala,
    resolvers += Resolver.mavenLocal,
    resolvers += Resolver.jcenterRepo,
    //resolvers += "GeneralJava" at "https://dy1.jfrog.io/artifactory/all-java/",
    //resolvers += "EventCollection" at "https://dy1.jfrog.io/artifactory/event-collection-common/",
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % versions.typesafe_config,
      "com.typesafe.scala-logging" %% "scala-logging" % versions.scala_logging,
      "org.typelevel" %% "cats-core" % versions.cats,
      "ch.qos.logback" % "logback-classic" % versions.logback,
      "mysql" % "mysql-connector-java" % versions.mysql,
      //"com.dynamicyield" % "event-collection-schema" % versions.coll_schema,
      //"com.dy.java" % "metrics" % versions.dy_metrics,
      "com.amazonaws" % "aws-java-sdk" % versions.aws_sdk_s3 % "provided",
      "org.apache.hadoop" % "hadoop-aws" % versions.hadoop_aws % "provided",
      "org.apache.spark" %% "spark-core" % versions.spark % "provided",
      "org.apache.spark" %% "spark-sql" % versions.spark % "provided",
      "org.apache.spark" %% "spark-avro" % versions.spark % "provided",
      "org.scalatest" %% "scalatest" % versions.tests.scalaTest % "test",
      "org.mockito" %% "mockito-scala-scalatest" % versions.tests.mockito % "test"
    )
  )