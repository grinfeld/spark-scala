package com.mikerusoft.spark.scala.iceberg

import org.apache.spark.sql.SparkSession

object ReadDataFromExistedTable extends App {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    // .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkSessionCatalog") for hive
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.sql.catalog.local.warehouse", "s3a://test-sink/warehouse/")
    .getOrCreate()

  spark.read.table("local.sparkbi")
    .show()


}
