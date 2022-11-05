package com.mikerusoft.spark.scala.iceberg

import org.apache.spark.sql.SparkSession

import java.io.File;

object IcebergCreateExampleTable extends App {
  // warehouseLocation points to the default location for managed databases and tables
  val warehouseLocation = new File("spark-warehouse")

/*  if (warehouseLocation.exists())
    FileUtils.deleteDirectory(warehouseLocation)*/

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("app")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", "s3a://test-sink")
    .getOrCreate()

  spark
    .sql("CREATE TABLE IF NOT EXISTS local.db.table (id bigint, data string) USING iceberg PARTITIONED BY (id)")


  //val sparkSql: SQLContext = spark.table("local.db.table").sqlContext
  //sparkSql.sql("INSERT INTO local.db.table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
  //sparkSql.sql("MERGE INTO local.db.target t USING (SELECT * FROM updates) u ON t.id = u.id\nWHEN MATCHED THEN UPDATE SET t.count = t.count + u.count\nWHEN NOT MATCHED THEN INSERT *")
  // sparkSql.sql("INSERT INTO local.db.table SELECT id, data FROM source WHERE length(data) = 1")


  println("---------")

  //spark.read.parquet(parq).show(10)
  //Thread.sleep(100000000)
}
