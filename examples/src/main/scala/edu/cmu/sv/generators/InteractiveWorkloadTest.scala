package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

/* 
 * generates an interactive workload
 * execute with: 
 * ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar 10 trace 1 /tmp/part-00000-of-00500.csv
 */
object InteractiveWorkloadTest {

  final val ITERATIONS = 50
  final val CONCURRENT_WORKLOADS = 5
  final val MIN_WAIT_TIME = 1000
  final val MAX_WAIT_TIME = 10000

  def main(args: Array[String]) {
  	
    val iterations = if (args.length > 0) args(0).toInt else ITERATIONS
    val conf = new SparkConf().setAppName("Interactive Workload").setUseBayes("true")
    implicit val spark = new SparkContext(conf)

    if (args.length > 1 && args(1) == "trace") {
      class TraceInteractiveWorkload extends InteractiveWorkload(iterations, CONCURRENT_WORKLOADS, MAX_WAIT_TIME, MIN_WAIT_TIME) with GoogleTraceTaskUsage {
        override val nreads = args(2).toInt
        // override val filepath = args(3)
      }
      val trace = new TraceInteractiveWorkload
      trace.concurrentWorkload()
    }
    else {
      class PiInteractiveWorkload extends InteractiveWorkload(iterations, CONCURRENT_WORKLOADS, MAX_WAIT_TIME, MIN_WAIT_TIME) with PiApproximation
      val pi = new PiInteractiveWorkload
      pi.concurrentWorkload()
    }

    Thread.sleep(20000)
    
    spark.stop()
  }
}
