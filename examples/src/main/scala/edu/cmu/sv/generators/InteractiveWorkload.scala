package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

abstract class InteractiveWorkload(iterations:Int, concurrentWorkload:Int, maxWaitTime:Int, minWaitTime:Int) extends WorkloadGenerator {

  final val MAX_RDDs = 5

  class Workload(_spark:SparkContext) extends Thread {

    implicit val spark = _spark
    var pi:Double = -1

    override def run {
      Thread.sleep(math.max(new Random().nextInt(maxWaitTime), minWaitTime))
      pi = generateWorkload(new Random().nextInt(MAX_RDDs))
    }
  }

  def concurrentWorkload()(implicit spark:SparkContext) {
    for(i <- 1 to iterations) {
      val threads = List.fill(concurrentWorkload)(new Workload(spark))
        threads.foreach(_.start)
        threads.foreach(_.join)
        threads.foreach { wl =>
          println(s"iteration $i : result is approximately: ${wl.pi}")
      }
    }
  }

}
