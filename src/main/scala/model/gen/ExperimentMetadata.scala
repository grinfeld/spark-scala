package com.dy.spark.scala
package model.gen

case class ExperimentMetadata(experimentId: Int, versionId: Int, variations: Option[List[Int]])