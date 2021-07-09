package com.mikerusoft.spark.scala
package com.mikerusoft.spark.scala.infra.spark

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object DatasetTypes {

  type SparkSessionType[C] = Flow[SparkSession, C, Dataset]
  type SparkSessionRowType = SparkSessionType[Row]
  type DatasetType[I, C] = Flow[Dataset[I], C, Dataset]
  type Dataset2RowType = DatasetType[Row, Row]
  type Tuple2DatasetType[I1,I2,C] = Flow[(Dataset[I1], Dataset[I2]), C, Dataset]
}
