package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

/* 
 * generates an iterative workload
 * execute with: 
 * ./bin/spark-submit --class edu.cmu.sv.generators.IterativeWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar 10 trace 1 /tmp/part-00000-of-00500.csv
 */
object IterativeWorkloadTest {

  final val ITERATIONS = 50
  final val SLEEP_MILLIS = 1000

  def main(args: Array[String]) {
  	
    val iterations = if (args.length > 0) args(0).toInt else ITERATIONS

    val conf = new SparkConf()
      .setAppName("Iterative Workload")
      .setUseBayes("false")      

    implicit val spark = new SparkContext(conf)

    if (args.length > 1 && args(1) == "trace") {
      class TraceIterativeWorkload extends IterativeWorkload(iterations, SLEEP_MILLIS) with GoogleTraceTaskUsage {
        override val nreads = args(2).toInt
        // override val filepath = args(3)
      }
      val trace = new TraceIterativeWorkload
      trace.iterativeWorkload()
    }
    else {
      class PiIterativeWorkload extends IterativeWorkload(iterations, SLEEP_MILLIS) with PiApproximation
      val pi = new PiIterativeWorkload
      pi.iterativeWorkload()
    }

    Thread.sleep(20000)
    
    spark.stop()
  }
}
