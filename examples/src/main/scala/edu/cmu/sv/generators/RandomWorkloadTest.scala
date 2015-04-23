package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

/* 
 * generates a random workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.RandomWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar 5
 *    you can also use --master local[512]
 */
object RandomWorkloadTest {

  final val RANDOM_ITERATIONS = 50
  final val ITERATIVE_ITERATIONS = 50
  final val ITERATIVE_SLEEP_MILLIS = 1000
  final val CONCURRENT_ITERATIONS = 1
  final val CONCURRENT_WORKLOADS = 5
  final val CONCURRENT_MIN_WAIT_TIME = 1000
  final val CONCURRENT_MAX_WAIT_TIME = 10000

  def main(args: Array[String]) {
  	
    val iterations = if (args.length > 0) args(0).toInt else RANDOM_ITERATIONS
    val conf = new SparkConf().setAppName("Random Workload")
    implicit val spark = new SparkContext(conf)

    if (args.length > 1 && args(1) == "trace") {
      class TraceInteractiveWorkload extends InteractiveWorkload(CONCURRENT_ITERATIONS, CONCURRENT_WORKLOADS, CONCURRENT_MIN_WAIT_TIME, CONCURRENT_MAX_WAIT_TIME) with GoogleTraceTaskUsage
      class TraceIterativeWorkload extends IterativeWorkload(ITERATIVE_ITERATIONS, ITERATIVE_SLEEP_MILLIS) with GoogleTraceTaskUsage
      class TraceRandomWorkload extends RandomWorkload(iterations, new TraceInteractiveWorkload(), new TraceIterativeWorkload())
      val trace = new TraceRandomWorkload
      trace.randomWorkload()
    }
    else {
      class PiInteractiveWorkload extends InteractiveWorkload(CONCURRENT_ITERATIONS, CONCURRENT_WORKLOADS, CONCURRENT_MIN_WAIT_TIME, CONCURRENT_MAX_WAIT_TIME) with PiApproximation
      class PiIterativeWorkload extends IterativeWorkload(ITERATIVE_ITERATIONS, ITERATIVE_SLEEP_MILLIS) with PiApproximation
      class PiRandomWorkload extends RandomWorkload(iterations, new PiInteractiveWorkload(), new PiIterativeWorkload())
      val pi = new PiRandomWorkload
      pi.randomWorkload()
    }

    Thread.sleep(10000)
    
    spark.stop()
  }
}
