package com.mikerusoft.spark.scala.iceberg

import org.apache.spark.sql.SparkSession

import java.io.File

object IcebergInsertFromExample extends App {

  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse")

  /*if (warehouseLocation.exists())
    FileUtils.deleteDirectory(warehouseLocation)*/

  //val s: TableCatalog = new SparkCatalog()
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouseLocation.getAbsolutePath)
    .getOrCreate()

  //spark.read.parquet(parq).repartition(functions.col("sectionId"))
  //spark.sql("CREATE TABLE local.db.table (id bigint, data string) USING iceberg")
  //spark.sql("INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  //spark.sql("INSERT INTO local.db.table SELECT id, data FROM source WHERE length(data) = 1")

  val sparkSql = spark.table("local.db.table").sqlContext
  sparkSql.sql("INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  sparkSql.sql("MERGE INTO local.db.target t USING (SELECT * FROM updates) u ON t.id = u.id\nWHEN MATCHED THEN UPDATE SET t.count = t.count + u.count\nWHEN NOT MATCHED THEN INSERT *")
  println("---------")

  //spark.read.parquet(parq).show(10)
  //Thread.sleep(100000000)
}
