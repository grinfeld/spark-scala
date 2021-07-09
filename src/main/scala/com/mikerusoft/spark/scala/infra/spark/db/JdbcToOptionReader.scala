package com.mikerusoft.spark.scala.infra.spark.db

import com.mikerusoft.spark.scala.apps.helpers.db.DbProps
import org.apache.spark.sql.DataFrameReader
import com.mikerusoft.spark.scala.infra.db.Reader

case class JdbcToOptionReader(reader: DataFrameReader, props: DbProps) extends Reader[DataFrameReader, DbProps] {
  override def setIf[T](name: String, func: DbProps => Option[T]): Reader[DataFrameReader, DbProps] =
    JdbcToOptionReader(func(props) match {
      case None => reader
      case Some(v) => v match {
        case i:Int => reader.option(name, i)
        case l:Long => reader.option(name, l)
        case s:String => reader.option(name, s)
        case v => throw new IllegalArgumentException("Unsupported type " + v.getClass)
      }
    }, props)

  override def setWith[T](name: String, func: DbProps => T): Reader[DataFrameReader, DbProps] =
    JdbcToOptionReader(func(props) match {
      case i:Int => reader.option(name, i)
      case l:Long => reader.option(name, l)
      case s:String => reader.option(name, s)
      case v => throw new IllegalArgumentException("Unsupported type " + v.getClass)
    }, props)

  override def get: DataFrameReader = reader

  override def set[T](name: String, t: T): Reader[DataFrameReader, DbProps] = {
    JdbcToOptionReader(t match {
      case i:Int => reader.option(name, i)
      case l:Long => reader.option(name, l)
      case s:String => reader.option(name, s)
      case v => throw new IllegalArgumentException("Unsupported type " + v.getClass)
    }, props)
  }
}
