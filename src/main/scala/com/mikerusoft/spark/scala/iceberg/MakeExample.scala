package com.mikerusoft.spark.scala.iceberg

import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

object MakeExample extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .config("spark.sql.catalog.metastore_db", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.metastore_db.type", "hive")
    .config("spark.sql.catalog.metastore_db.uri", "thrift://localhost:9083")
    .config("spark.sql.catalog.metastore_db.clients", "10")
    .config("spark.sql.catalog.metastore_db.warehouse", "s3a://test")
    //.config("spark.sql.catalog.local.warehouse", warehouseLocation.getAbsolutePath)
  .getOrCreate()

  val dataset = spark.read.parquet("s3a://test-sink/example.parquet")
    .withColumn("dt", col("unixTs").cast("timestamp"))
  val parquetSchema = dataset.schema
  dataset.show(1)

  val catalog = new HiveCatalog()
  catalog.initialize("sparkbi", new CaseInsensitiveStringMap(Map[String,String](
    ("type", "hive"),
    ("uri", "thrift://localhost:9083"),
    ("warehouse", "s3a://test")
  ).asJava))
  val finalSchema = SparkSchemaUtil.convert(parquetSchema, true)

  val spec: PartitionSpec = PartitionSpec.builderFor(finalSchema)
    .identity("sectionId")
    .hour("dt")
    .identity("experimentId")
    .identity("versionId")
    .build();
  catalog.createTable(TableIdentifier.of("metastore_db", "sparkbi"), finalSchema, spec)

  spark.read.parquet("s3a://test-sink/example.parquet")
    .withColumn("dt", col("unixTs").cast("timestamp"))
    .writeTo("metastore_db.sparkbi")
    .using("ICEBERG")
    .partitionedBy(
      col("sectionId"),
      hours(col("dt")),
      col("experimentId"),
      col("versionId")
    )
    .createOrReplace()
}
