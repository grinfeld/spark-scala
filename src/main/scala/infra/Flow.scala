package com.dy.spark.scala
package infra

import scala.language.higherKinds

trait Flow[I, C, F[_]] {
    def execution: I => F[C]
    def map[D](func: F[C] => F[D]):  Flow[I, D, F]
    def concat[B](flow: Flow[F[C], B, F]): Flow[I, B, F]
}

abstract class PairFlow[I1, I2, C, F[_]](val func: (I1, I2) => F[C]) extends Flow[(I1, I2), C, F] {
    override def execution: ((I1, I2)) => F[C] = pair => func(pair._1, pair._2)
}

abstract class FPairFlow[I1, I2, C, F[_]](val func: (F[I1],F[I2]) => F[C]) extends Flow[(F[I1],F[I2]), C, F] {
    override def execution: ((F[I1], F[I2])) => F[C] = pair => func(pair._1, pair._2)
}

trait FlowOutput[C, O, F[_]] {
    def output(fc: F[C]): O
}