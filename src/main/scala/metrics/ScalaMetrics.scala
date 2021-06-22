package com.dy.spark.scala
package metrics

import com.dy.metrics.functions.{CallExecution, Execution}
import com.dy.metrics.{MetricStore, Tag}

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

sealed trait ScalaMetrics {
  def increaseCounter(name: String, error: Boolean, value: Long, tags: Tag*): Unit
  def increaseCounter(name: String, runnable: () => Unit, tags: Tag*): Unit
  def increaseCounter(name: String, error: Boolean, tags: Tag*): Unit
  def increaseCounter[V](name: String, callable: () => V, tags: Tag*): V

  def recordTime(name: String, error: Boolean, duration: Duration, tags: Tag*): Unit
  def recordTime(name: String, runnable: () => Unit, tags: Tag*): Unit
  def recordTime(name: String, error: Boolean, start: Long, tags: Tag*): Unit
  def recordTime[V](name: String, callable: () => V, tags: Tag*): V

  def gauge(name: String, value: Double, error: Boolean, tags: Tag*): Unit
  def gauge[V](name: String, value: Double, callable: () => V, tags: Tag*): V
  def gauge(name: String, value: Double, runnable: () => Unit, tags: Tag*): Unit
  def removeGauge(name: String, error: Boolean, tags: Tag*): Unit
}

private class ScalaMetricsWrapper(metricStore: MetricStore) extends ScalaMetrics {

  private def runExecution(runnable: () => Unit): Execution = () => runnable.apply()
  private def runCallableExecution[V](callable: () => V): CallExecution[V] = () => callable.apply()

  override def increaseCounter(name: String, error: Boolean, value: Long, tags: Tag*): Unit = metricStore.increaseCounter(name, error, value, tags: _*)
  override def increaseCounter(name: String, runnable: () => Unit, tags: Tag*): Unit = metricStore.increaseCounter(name, runExecution(runnable), tags: _*)
  override def increaseCounter(name: String, error: Boolean, tags: Tag*): Unit = metricStore.increaseCounter(name, error, tags: _*)
  override def increaseCounter[V](name: String, callable: () => V, tags: Tag*): V = metricStore.increaseCounter(name, runCallableExecution(callable), tags: _*)

  override def recordTime(name: String, error: Boolean, duration: Duration, tags: Tag*): Unit = metricStore.recordTime(name, error, duration, tags: _*)
  override def recordTime(name: String, runnable: () => Unit, tags: Tag*): Unit = metricStore.recordTime(name, runExecution(runnable), tags: _*)
  override def recordTime(name: String, error: Boolean, start: Long, tags: Tag*): Unit = metricStore.recordTime(name, error, start, tags: _*)
  override def recordTime[V](name: String, callable: () => V, tags: Tag*): V = metricStore.recordTime(name, runCallableExecution(callable), tags: _*)

  override def gauge(name: String, value: Double, error: Boolean, tags: Tag*): Unit = metricStore.gauge(name, value, error, tags: _*)
  override def gauge[V](name: String, value: Double, callable: () => V, tags: Tag*): V = metricStore.gauge(name, value, runCallableExecution(callable), tags: _*)
  override def gauge(name: String, value: Double, runnable: () => Unit, tags: Tag*): Unit = metricStore.gauge(name, value, runExecution(runnable), tags: _*)
  override def removeGauge(name: String, error: Boolean, tags: Tag*): Unit = metricStore.removeGauge(name, error, tags: _*)

}

object ScalaMetrics extends ScalaMetrics {

  private val defMetricStore: MetricStore = new MetricStore() {
    override def increaseCounter(name: java.lang.String, error: java.lang.Boolean, value: Long, tags: Tag*): Unit = {}
    override def recordTime(name: java.lang.String, error: java.lang.Boolean, duration: Duration, tags: Tag*): Unit = {}
    override def gauge(name: java.lang.String, value: java.lang.Double, error: java.lang.Boolean, tags: Tag*): Unit = {}
  }

  private val initialized: AtomicBoolean = new AtomicBoolean(false)
  private var metricStore: ScalaMetrics = _

  def apply(): ScalaMetrics = {
    apply(defMetricStore)
  }

  def apply(_store: => MetricStore): ScalaMetrics = {
    if (initialized.getAndSet(true)) {
      metricStore = new ScalaMetricsWrapper(_store)
    }
    metricStore
  }

  def increaseCounter(name: String, error: Boolean, value: Long, tags: Tag*): Unit = apply().increaseCounter(name, error, value, tags: _*)
  def increaseCounter(name: String, runnable: () => Unit, tags: Tag*): Unit = apply().increaseCounter(name, runnable, tags: _*)
  def increaseCounter(name: String, error: Boolean, tags: Tag*): Unit = apply().increaseCounter(name, error, tags: _*)
  def increaseCounter[V](name: String, callable: () => V, tags: Tag*): V = apply().increaseCounter(name, callable, tags: _*)

  def recordTime(name: String, error: Boolean, duration: Duration, tags: Tag*): Unit = apply().recordTime(name, error, duration, tags: _*)
  def recordTime(name: String, runnable: () => Unit, tags: Tag*): Unit = apply().recordTime(name, runnable, tags: _*)
  def recordTime(name: String, error: Boolean, start: Long, tags: Tag*): Unit = apply().recordTime(name, error, start, tags: _*)
  def recordTime[V](name: String, callable: () => V, tags: Tag*): V = apply().recordTime(name, callable, tags: _*)

  def gauge(name: String, value: Double, error: Boolean, tags: Tag*): Unit = apply().gauge(name, value, error, tags: _*)
  def gauge[V](name: String, value: Double, callable: () => V, tags: Tag*): V = apply().gauge(name, value, callable, tags: _*)
  def gauge(name: String, value: Double, runnable: () => Unit, tags: Tag*): Unit = apply().gauge(name, value, runnable, tags: _*)
  def removeGauge(name: String, error: Boolean, tags: Tag*): Unit = apply().removeGauge(name, error, tags: _*)
}
