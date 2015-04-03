package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

/** generates an interactive workload */
object InteractiveWorkloadTest {

  final val CONCURRENT_WORKLOADS = 5
  final val MAX_WAIT_TIME = 5000

  class Workload(_spark:SparkContext) extends Thread with PiApproximation {

    implicit val spark = _spark
    var pi:Double = -1

    override def run {
      Thread.sleep(new Random(MAX_WAIT_TIME).nextInt)
      pi = approximatePi
    }
  }

  def main(args: Array[String]) {
  	
    val conf = new SparkConf().setAppName("Interactive Workload")
    implicit val spark = new SparkContext(conf)

    val threads = List.fill(CONCURRENT_WORKLOADS)(new Workload(spark))
    threads.foreach(_.start)
    threads.foreach(_.join)
    threads.foreach { wl =>
      println(s"pi is approximately: $wl.pi")
    }

    spark.stop()
  }
}
