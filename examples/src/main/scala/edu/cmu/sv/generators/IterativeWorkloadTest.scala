package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._

/* 
 * generates an iterative workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.IterativeWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
object IterativeWorkloadTest extends PiApproximation {

  final val ITERATIONS = 5000
  final val SLEEP_MILLIS = 1000

  def iterativeWorkload(iterations:Int, sleepMillis:Int)(implicit spark:SparkContext) {
    for(i <- 1 to iterations) {
      println(s"iteration $i - Pi is roughly: " + approximatePi)
      Thread.sleep(sleepMillis)
    }
  }

  def main(args: Array[String]) {
  	
    val conf = new SparkConf().setAppName("Iterative Workload")
    implicit val spark = new SparkContext(conf)
    
    //every second for 5000 iterations
    iterativeWorkload(ITERATIONS, SLEEP_MILLIS)

    spark.stop()
  }
}
