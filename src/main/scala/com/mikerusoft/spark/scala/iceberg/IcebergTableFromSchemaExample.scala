package com.mikerusoft.spark.scala.iceberg

import com.dy.experiment.ExperimentEvent
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.shaded.org.apache.avro.Schema
import org.apache.iceberg.shaded.org.apache.avro.Schema.{Field, Parser, Type}

import java.io.File
import scala.collection.JavaConverters._

object IcebergTableFromSchemaExample extends App {

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse")

  /*if (warehouseLocation.exists())
    FileUtils.deleteDirectory(warehouseLocation)*/

/*  def findMax(schema: Schema): Int = {
    val cols = schema.columns().asScala.map(t => t).toList
    cols match {
      case head:: Nil =>
      case head:: remainder =>
    }
  }*/

  val avroSchema: Schema = new Parser().parse(ExperimentEvent.SCHEMA$.toString())
  val fields = new Field("dt", Schema.create(Type.STRING))  :: new Field("unixTsMs", Schema.create(Type.LONG)) :: avroSchema.getFields.asScala.toList
  avroSchema.setFields(fields.asJava)
  val schema = AvroSchemaUtil.toIceberg(avroSchema)

  //val maxId = schema.identifierFieldIds().asScala.foldLeft(0)((s, max) => Math.max(s, max))
  /*val icebergSchema: List[Types.NestedField] = schema.columns().asScala.map(t => t).toList

  val list = icebergSchema.::(Types.NestedField.required(100, "unixTsMs", TimestampType.withoutZone()))
  val finalSchema = new org.apache.iceberg.Schema(list.asJava)*/

  val spec: PartitionSpec = PartitionSpec.builderFor(schema)
    .identity("sectionId")
    .hour("dt")
    .identity("experimentId")
    .identity("versionId")
  .build();


  //val s: TableCatalog = new SparkCatalog()
  /*val spark = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouseLocation.getAbsolutePath)
    .getOrCreate()*/

  /*val parquetFile = ClassLoader.getSystemResource("experiment.parquet").getFile
  val rows = spark.read.parquet(parquetFile)*/

//  import org.apache.iceberg.hadoop.HadoopTables
//
//  val conf = new Configuration()
//  val tables = new HadoopTables(conf)
//  val table = tables.create(schema, spec, warehouseLocation.getAbsolutePath)



//  val ut = new AvroSchemaConverter
//  val sc: MessageType = ut.convert(new Parser().parse(ExperimentEvent.SCHEMA$.toString()))


//  val s: TableCatalog = new SparkCatalog()
//  val ops: util.Map[String, String] = Map[String, String]().asJava

    /*spark.read.parquet(parquetFile).limit(1).writeTo("local.db.sparkbi")
      .using("ICEBERG").create()*/

  //ALTER TABLE prod.db.sample ADD PARTITION FIELD catalog -- identity transform
/*    spark.table("local.db.sparkbi")
      .writeTo("local.db.sparkbi")
      .using("ICEBERG")
      .partitionedBy(
        col("sectionId"),
        hours(col("dt")),
        col("experimentId"),
        col("versionId")
      )
      .createOrReplace()*/

  /*rows
      .writeTo("local.db.sparkbi")
      .using("ICEBERG")
      .partitionedBy(
        col("sectionId"),
        hours(col("dt")),
        col("experimentId"),
        col("versionId")
      )
      .createOrReplace()

    spark.read.table("local.db.sparkbi")
      .select("*").show(10)

  spark.read.table("local.db.sparkbi.history")
    .select("*").show(10)*/
}
