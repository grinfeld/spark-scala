package com.dy.spark.scala
package infra.spark

import org.apache.spark.sql.Row

import scala.collection.Map

object RowWrapper {
  implicit class RowWrapper(row: Row) {
    private lazy val nameToIndex: Map[String, Int] = row.schema.fieldNames.zipWithIndex.toMap
    def getAsOption[T](name: String): Option[T] = nameToIndex.get(name).map(ind => row.getAs(ind).asInstanceOf[T])
    def getAsList[T](name: String): List[T] = getAsOption[List[T]](name).getOrElse(List[T]())
  }
}
