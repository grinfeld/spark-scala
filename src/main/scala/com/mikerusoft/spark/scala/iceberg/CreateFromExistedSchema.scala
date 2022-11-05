package com.mikerusoft.spark.scala.iceberg

import com.dy.experiment.ExperimentEventSpark
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.{PartitionSpec, Table}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, functions}

import scala.util.{Failure, Success, Try}

object CreateFromExistedSchema extends App {

  /*val avroSchemaOrig = new Parser().parse(ExperimentEventSpark.SCHEMA$.toString())
  val schema = AvroSchemaUtil.toIceberg(avroSchemaOrig)*/

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    /*
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
       --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
       --conf spark.sql.catalog.spark_catalog.type=hive \
       --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
       --conf spark.sql.catalog.local.type=hadoop \
       --conf spark.sql.catalog.local.warehouse=$PWD/warehouse
    */
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

  import spark.implicits._

  val parquetSchema = SchemaConverters.toSqlType(ExperimentEventSpark.SCHEMA$).dataType.asInstanceOf[StructType]
  val icebergSchema = SparkSchemaUtil.convert(parquetSchema)

  val catalog = new HadoopCatalog(spark.sparkContext.hadoopConfiguration, "s3a://test-sink/warehouse/")
  val spec = PartitionSpec.builderFor(icebergSchema)
    .identity("sectionId")
    //.month("unixTs")
    .day("unixTs")
    .identity("experimentId")
    .identity("versionId")
  .build()

  val tableIdentifier = TableIdentifier.parse("sparkbi")
  val loadedTable: Table = Try(catalog.loadTable(tableIdentifier)) match {
    case Success(table) => table
    case Failure(_: NoSuchTableException) => catalog.createTable(tableIdentifier, icebergSchema, spec)
    case Failure(e) => throw e
  }

  println(loadedTable.name())

  val ds = spark.read
    //.schema(parquetSchema)
    .parquet("s3a://test-sink/data/")

  //val sp: Schema = SparkSchemaUtil.convert(ds.schema)
    ds
      .withColumn("tmpDate", functions.to_date(functions.from_unixtime(functions.col("unixTs"))))
      .drop("unixTs").withColumnRenamed("tmpDate", "unixTs")
/*
      .withColumn("vars", when($"variations".isNull, typedLit(Seq.empty[Int])).otherwise($"variations"))
      .drop("variations").withColumnRenamed("vars", "variations")
      .withColumn("auds", when($"audiences".isNull, typedLit(Seq.empty[Int])).otherwise($"audiences"))
      .drop("audiences").withColumnRenamed("auds", "audiences")
      .withColumn("uns", when($"units".isNull, typedLit(Seq.empty[Int])).otherwise($"units"))
      .drop("units").withColumnRenamed("uns", "units")
      .withColumn("sumMechs", when($"subMechanisms".isNull, typedLit(Seq.empty[Int])).otherwise($"subMechanisms"))
      .drop("subMechanisms").withColumnRenamed("sumMechs", "subMechanisms")
*/
      .sortWithinPartitions(
        "sectionId",
    //    "month",
        "day",
        "experimentId",
        "versionId"
      )
      .writeTo("local.sparkbi")
      .append()

    /*.using("ICEBERG")
      .partitionedBy(
        column("sectionId"),
        column("month"),
        column("experimentId"),
        column("versionId"),
        column("day")
     )*/


}
