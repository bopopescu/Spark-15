package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import org.apache.spark.rdd._
import scala.util.Try
import scala.collection.mutable.HashMap
import java.io.File

abstract class WorkloadGenerator extends java.io.Serializable {
  def generateWorkload(rddId:Int)(implicit spark:SparkContext): Double
}

/** Computes an approximation to pi */
trait PiApproximation extends WorkloadGenerator {

	//org.apache.spark.examples.SparkPi

	def generateWorkload(rddId:Int)(implicit spark:SparkContext) = {
		// val slices = if (args.length > 0) args(0).toInt else 2
		val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    4.0 * count / n  
	}

}

object GoogleTraceTaskUsage {
  val rdds = HashMap[Int, RDD[_]]()
}

trait GoogleTraceTaskUsage extends WorkloadGenerator {

  import GoogleTraceTaskUsage._

  val nreads = 1
  val filepath = "/tmp/part-00000-of-00500.csv"
  private val file = "file://" + filepath

  case class TaskUsage(
      startTime:Long,
      endtime:Long,
      jobID:Long,
      taskIndex:Long,
      machineId:Long,
      meanCPUUsageTime:Double,
      canonicalMemoryUsage:Double,
      assignedMemoryUsage:Double,
      unamppedPageUsage:Double,
      totalPageCacheMemoryUsage:Double,
      maxMemoryUsage:Double,
      meanDiskIOTime:Double,
      meanLocalDiskSpaceDouble:Double,
      maxCPUUsage:Double,
      maxDiskIIOTime:Double,
      cPI:Double,
      memAccessPerInstruction:Double,
      samplePortion:Double,
      aggregationType:Double,
      sampledCPUUsage:Double)

  private def generateRDD()(implicit spark:SparkContext):RDD[_] = {

    val rdd = (1 to nreads).toList.foldLeft(spark.textFile(file)) { (r, i) =>
      if (!new File(filepath).exists()) {
        val newFile = new File(filepath)
        newFile.createNewFile()
      }
      val tmp = spark.textFile(file)
      r ++ tmp
    }

    val taskUsage = rdd.map(_.split(",")).flatMap(row => Try(TaskUsage(
      row(0).toLong, row(1).toLong, row(2).toLong, row(3).toLong, row(4).toLong, row(5).toDouble, row(6).toDouble, row(7).toDouble, row(8).toDouble,
      row(9).toDouble, row(10).toDouble, row(11).toDouble, row(12).toDouble, row(13).toDouble, row(14).toDouble, row(15).toDouble, row(16).toDouble,
      row(17).toDouble, row(18).toDouble, row(19).toDouble)).toOption
    )

    taskUsage.persist()    
  }

  def generateWorkload(rddId:Int)(implicit spark:SparkContext):Double = {
    val rdd = rdds.getOrElseUpdate(rddId, generateRDD)
    rdd.asInstanceOf[RDD[TaskUsage]].map(_.meanCPUUsageTime).mean()
    rdd.count()
  }

}