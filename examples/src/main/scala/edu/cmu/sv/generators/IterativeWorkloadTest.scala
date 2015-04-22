package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

/* 
 * generates an iterative workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.IterativeWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar 5
 *    you can also use --master local[512]
 */
object IterativeWorkloadTest {

  final val ITERATIONS = 50
  final val SLEEP_MILLIS = 1000

  def main(args: Array[String]) {
  	
    val iterations = if (args.length > 0) args(0).toInt else ITERATIONS
    val conf = new SparkConf().setAppName("Iterative Workload")
    implicit val spark = new SparkContext(conf)

    class PiIterativeWorkload extends IterativeWorkload(iterations, SLEEP_MILLIS) with PiApproximation
    val pi = new PiIterativeWorkload
    pi.iterativeWorkload()

    spark.stop()
  }
}
