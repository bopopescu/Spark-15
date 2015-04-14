package org.apache.spark.storage

import java.util.LinkedHashMap
import java.util.LinkedList
import java.io._
import java.lang.System
import scala.math.random
import org.apache.spark._
import scala.util.Random

import java.io.PrintWriter

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
class csvGenerator(usage: LinkedHashMap[BlockId, LinkedList[Long]], hitMiss: LinkedHashMap[BlockId, LinkedList[Boolean]]) extends Thread{

  //val usage = _usage
  //val hitMiss = _hitMiss
  
  override def run {
    println(s"CMU - Usage information written to csv file ")
    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("usageHistory.csv", true)))

    println(s"CMU - Usage information written to csv file, time: " + String.valueOf(System.currentTimeMillis()))
    
    while(true) {
      val currTime = System.currentTimeMillis()
      println(s"######################################## time: " + currTime + " ########################################")
      println(s"######################################## usage size: " + String.valueOf(usage.size) + " ########################################")
      println(s"######################################## hitMiss size: " + String.valueOf(hitMiss.size) + " ########################################")
      // val iteratorU = usage.entrySet().iterator()
      // while (iteratorU.hasNext) {
      //   var str = ""
      //   val pair = iterator.next()
      //   val blockId = pair.getKey
      //   val freq = pair.getValue.size
      //   str = str + blockId + "," + freq + "\n"
      //   out.write(str)
    }
    Thread.sleep(1000)
    out.close()
  } 
}
