package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

abstract class IterativeWorkload(iterations:Int, sleepMillis:Int) extends WorkloadGenerator {

	final val MAX_RDDs = 2

  def iterativeWorkload()(implicit spark:SparkContext) {
    for(i <- 1 to iterations) {
    	for(y <- 1 to MAX_RDDs) {
    		println(s"iteration $i.$y - result is roughly: " + generateWorkload(y))	      
    	}
      Thread.sleep(sleepMillis)
    }
  }

}
