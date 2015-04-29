package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

abstract class IterativeWorkload(iterations:Int, sleepMillis:Int) extends WorkloadGenerator {

	final val MAX_RDDs = 5

	class Workload(_spark:SparkContext) extends Thread {

    implicit val spark = _spark
    var pi:Double = -1

    override def run {
    	for(y <- 1 to MAX_RDDs) {
    		println(s"iteration $y - result is roughly: " + generateWorkload(y))
    	}      
    }
  }

  def iterativeWorkload()(implicit spark:SparkContext) {
  	val threads = List.fill(iterations)(new Workload(spark))
  	threads.foreach(_.start)
    threads.foreach(_.join)    
  }

}
