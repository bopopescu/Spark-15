package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

/* 
 * generates a random workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.RandomWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
object RandomWorkloadTest extends PiApproximation {

  final val RANDOM_ITERATIONS = 5000
  final val ITERATIVE_ITERATIONS = 100
  final val ITERATIVE_SLEEP_MILLIS = 1000
  final val CONCURRENT_ITERATIONS = 1
  final val CONCURRENT_WORKLOADS = 5
  final val CONCURRENT_MIN_WAIT_TIME = 1000
  final val CONCURRENT_MAX_WAIT_TIME = 10000

  def main(args: Array[String]) {
  	
    val conf = new SparkConf().setAppName("Random Workload")
    implicit val spark = new SparkContext(conf)

    for(i <- 1 to RANDOM_ITERATIONS) {
      if(new Random().nextDouble() >= 0.5)
        InteractiveWorkloadTest.concurrentWorkload(CONCURRENT_ITERATIONS, CONCURRENT_WORKLOADS, CONCURRENT_MIN_WAIT_TIME, CONCURRENT_MAX_WAIT_TIME)
      else
        IterativeWorkloadTest.iterativeWorkload(ITERATIVE_ITERATIONS, ITERATIVE_SLEEP_MILLIS)
    }

    spark.stop()
  }
}
