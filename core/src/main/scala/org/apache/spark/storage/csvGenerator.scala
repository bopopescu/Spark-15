package org.apache.spark.storage

import java.util.LinkedHashMap
import java.util.LinkedList
import java.io._
import java.lang.System
import scala.math.random
import org.apache.spark._
import scala.util.Random
import scala.util.control.Breaks._
import java.nio.file.{Paths, Files}
import java.io.PrintWriter

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
class CsvGenerator(entries:EnrichedLinkedHashMap[BlockId, MemoryEntry], jobName:String) extends Thread {
 
  override def run {

    var count = 0
    var preLastTime = 0L
    var index = 0
    
    var usageOutputFileName = "segment1.data"
    if(jobName == "Iterative Workload")
      usageOutputFileName = "segment1.data"
    else if (jobName == "Interactive Workload")
      usageOutputFileName = "segment2.data"
    else if(jobName == "Combination Workload")
      usageOutputFileName = "segment3.data"
    
    println(s"CMU - Usage information written to csv file ")

    try{
      val inHitRate = new BufferedReader(new InputStreamReader(new FileInputStream("HitRate.txt")))
      var str = inHitRate.readLine()
      println(s"######################################## Name: " + getName() + " ########################################")
      while(str != null) {
        str = str.trim()
        if(str.length() > 0) {
          var strArr = str.split("\t")
          index = strArr(0).toInt
        }
        str = inHitRate.readLine()
      }
      index = index + 1
      inHitRate.close()
    } 
    catch{
      case ex : IOException => {
        println(s"######################################## Creating Hitrate.txt ########################################")
        val newFile = new File("HitRate.txt")
        newFile.createNewFile()
        index = index + 1
      }
    }

    val dirName = "usageHistoryRecord"
    val fileName = dirName + "/usageHistory_" + index + ".csv"

    val dir = new File(dirName)
    if(!dir.exists()) {
      dir.mkdirs()
    }

    val out_record = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(usageOutputFileName))))

    println(s"CMU - Usage information written to csv file, time: " + String.valueOf(System.currentTimeMillis()))
    
    out.write("1,1,1,1\n")
    out_record.write("1,1,1,1\n")
    out.flush()
    out_record.flush()
    
    breakable {
      while(true) {
        entries.synchronized {
          val currTime = System.currentTimeMillis()
          var lastTime = entries.lastEntryAccessTime
          println(s"######################################## Name: " + getName() + " ########################################")
          println(s"######################################## time: " + currTime + " ########################################")
          println(s"######################################## usage size: " + String.valueOf(entries.usage.size) + " ########################################")
          println(s"######################################## hitMiss size: " + String.valueOf(entries.hitMiss.size) + " ########################################")
          println(s"######################################## count: " + String.valueOf(count) + " ########################################")

          val iteratorU = entries.usage.toIterator
          while (iteratorU != null && iteratorU.hasNext) {
            var str = ""
            val (blockId, usages) = iteratorU.next()          
            val time1 = usages(0)
            val size = usages.size
            val freq = 1000.0 * size / (currTime - time1)
            val blockEntries = entries.getNoUsage(blockId)
            if (blockEntries != null) {
              val blockSize = blockEntries.size
              val ratio = 1.0 * usages(size-1) / currTime
              val label = new Random().nextDouble() * 100
              str = str + freq + "," + blockSize + "," + ratio + "," + label + "\n"
              out.write(str)
              out_record.write(str)
              out.flush()
              out_record.flush()
            }
          }

          if(preLastTime == lastTime)
            count = count + 1
          else 
            count = 0;

          if(count >= 10)
            break

          preLastTime = lastTime          
        }
        Thread.sleep(1000)
      }
    }
    out.close()
    out_record.close()

    //calculate the hit rate
    println(s"######################################## Writing Hitrate ########################################")
    entries.synchronized {
      val outHitRate = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("HitRate.txt", true)))
      val iteratorH = entries.hitMiss.toIterator
      var hitCount = 0
      var totalSize = 0
      while(iteratorH.hasNext) {
        val (blockId, list) = iteratorH.next()
        totalSize = totalSize + list.size
        for(i <- 0 until list.size) {
          if(list(i) == true){
            hitCount = hitCount + 1
          }
        }
      }
      val hitRate = 1.0 * hitCount / totalSize
      var hitRateRst = ""
      
      hitRateRst = hitRateRst + index + "\t" + jobName + "\t" + hitRate + "\n"
      outHitRate.write(hitRateRst)
      println(s"######################################## Finish Writing Hitrate ########################################")
      outHitRate.close()
    }
  }
}
