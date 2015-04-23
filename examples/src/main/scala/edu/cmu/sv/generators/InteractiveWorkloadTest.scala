package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar 5
 *    you can also use --master local[512]
 */
object InteractiveWorkloadTest {

  final val ITERATIONS = 1000
  final val CONCURRENT_WORKLOADS = 5
  final val MIN_WAIT_TIME = 1000
  final val MAX_WAIT_TIME = 10000

  def main(args: Array[String]) {
  	
    val iterations = if (args.length > 0) args(0).toInt else ITERATIONS
    val conf = new SparkConf().setAppName("Interactive Workload")
    implicit val spark = new SparkContext(conf)

    if (args.length > 1 && args(1) == "trace") {
      class TraceInteractiveWorkload extends InteractiveWorkload(iterations, CONCURRENT_WORKLOADS, MAX_WAIT_TIME, MIN_WAIT_TIME) with GoogleTraceTaskUsage
      val trace = new TraceInteractiveWorkload
      trace.concurrentWorkload()
    }
    else {
      class PiInteractiveWorkload extends InteractiveWorkload(iterations, CONCURRENT_WORKLOADS, MAX_WAIT_TIME, MIN_WAIT_TIME) with PiApproximation
      val pi = new PiInteractiveWorkload
      pi.concurrentWorkload()
    }

    Thread.sleep(10000)
    
    spark.stop()
  }
}
