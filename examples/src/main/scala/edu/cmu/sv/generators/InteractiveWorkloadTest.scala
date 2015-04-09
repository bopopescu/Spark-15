package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
object InteractiveWorkloadTest {

  final val ITERATIONS = 1000
  final val CONCURRENT_WORKLOADS = 5
  final val MIN_WAIT_TIME = 1000
  final val MAX_WAIT_TIME = 10000

  class Workload(_spark:SparkContext) extends Thread with PiApproximation {

    implicit val spark = _spark
    var pi:Double = -1

    override def run {
      Thread.sleep(math.max(new Random(MAX_WAIT_TIME).nextInt, MIN_WAIT_TIME))
      pi = approximatePi
    }
  }

  def main(args: Array[String]) {
  	
    val conf = new SparkConf().setAppName("Interactive Workload")
    implicit val spark = new SparkContext(conf)

    for(i <- 1 to ITERATIONS) {
      val threads = List.fill(CONCURRENT_WORKLOADS)(new Workload(spark))
      threads.foreach(_.start)
      threads.foreach(_.join)
      threads.foreach { wl =>
        println(s"iteration $i : pi is approximately: ${wl.pi}")
      }
    }

    spark.stop()
  }
}
