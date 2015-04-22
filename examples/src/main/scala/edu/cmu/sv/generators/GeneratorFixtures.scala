package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

abstract class WorkloadGenerator {
  def generateWorkload()(implicit spark:SparkContext): Double
}

/** Computes an approximation to pi */
trait PiApproximation extends WorkloadGenerator {

	//org.apache.spark.examples.SparkPi

	def generateWorkload()(implicit spark:SparkContext) = {
		// val slices = if (args.length > 0) args(0).toInt else 2
		val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    4.0 * count / n  
	}

}