package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

/** generates an iterative workload */
object IterativeWorkloadTest extends PiApproximation {

  final val ITERATIONS = 5

  def main(args: Array[String]) {
  	
    val conf = new SparkConf().setAppName("Iterative Workload")
    implicit val spark = new SparkContext(conf)
    
    for(i <- 0 to ITERATIONS-1) {
    	println(s"iteration $i - Pi is roughly: " + approximatePi)
    }

    spark.stop()
  }
}
