package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

abstract class IterativeWorkload(iterations:Int, sleepMillis:Int) extends WorkloadGenerator {

  def iterativeWorkload()(implicit spark:SparkContext) {
    for(i <- 1 to iterations) {
      println(s"iteration $i - result is roughly: " + generateWorkload)
      Thread.sleep(sleepMillis)
    }
  }

}
