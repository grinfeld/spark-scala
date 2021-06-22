package com.dy.spark.scala
package infra.db

trait Reader[E, C] {
  def setIf[T](name: String, func: C => Option[T]): Reader[E, C]
  def setWith[T](name: String, func: C => T): Reader[E, C]
  def set[T](name: String, t: T): Reader[E, C]
  def get: E
}
