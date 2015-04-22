package org.apache.spark.storage

import java.util.LinkedHashMap
import java.util.LinkedList
import java.io._
import java.lang.System
import scala.math.random
import org.apache.spark._
import scala.util.Random
import scala.util.control.Breaks._

import java.io.PrintWriter

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
class csvGenerator(usage: LinkedHashMap[BlockId, LinkedList[Long]], hitMiss: LinkedHashMap[BlockId, LinkedList[Boolean]],
  entries: LinkedHashMap[BlockId, MemoryEntry], lastEntryAccessTime: LinkedList[Long]) extends Thread{

  //val usage = _usage
  //val hitMiss = _hitMiss
  
  override def run {
    synchronized{
      println(s"CMU - Usage information written to csv file ")

      var count = 0
      var preLastTime = 0L
      var index = 0

      try{
        val inHitRate = new BufferedReader(new InputStreamReader(new FileInputStream("HitRate.txt")))
        var str = inHitRate.readLine()
        while(str != null) {
          println(s"######################################## Name: " + getName() + " ########################################" + index)
          str = str.trim()
          if(str.length() > 0) {
            var strArr = str.split("\t")
            index = strArr(0).toInt
          }
          str = inHitRate.readLine()
        }
        index = index + 1
        inHitRate.close()
      } catch{
        case ex : IOException => {
          println(s"######################################## Creating Hitrate.txt ########################################" + index)
          val newFile = new File("HitRate.txt")
          newFile.createNewFile()
          index = index + 1
        }
      }
      
      //val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("usageHistory.csv")))
      
      println(s"CMU - Usage information written to csv file, time: " + String.valueOf(System.currentTimeMillis()))
      
      val dirName = "usageHistoryRecord"
      val fileName = dirName + "/usageHistory_" + index + ".csv"

      val dir = new File(dirName)
      if(!dir.exists()) {
        dir.mkdirs()
      }
      val out_record = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)))
      val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("segment.data")))



      breakable{
        while(true) {
          val currTime = System.currentTimeMillis()
          var lastTime = lastEntryAccessTime.get(0)
          println(s"######################################## Name: " + getName() + " ########################################")
          println(s"######################################## time: " + currTime + " ########################################")
          println(s"######################################## usage size: " + String.valueOf(usage.size) + " ########################################")
          println(s"######################################## hitMiss size: " + String.valueOf(hitMiss.size) + " ########################################")
          println(s"######################################## count: " + String.valueOf(count) + " ########################################")
          usage.synchronized{
            val iteratorU = usage.entrySet().iterator()
            while (iteratorU.hasNext) {
              var str = ""
              val pair = iteratorU.next()
              val blockId = pair.getKey
              val time1 = pair.getValue.get(0)
              val count = pair.getValue.size
              val freq = 1000.0 * count / (currTime - time1)
              val blockSize = entries.get(blockId).size
              val ratio = 1.0 * pair.getValue.get(count-1) / currTime
              val label = new Random().nextInt(100)
              str = str + freq + "," + blockSize + "," + ratio + "," + label + "\n"
              out.write(str)
              out_record.write(str)
            }
          }
          
          if(preLastTime == lastTime)
            count = count + 1
          else 
            count = 0;
          if(count >= 10)
            break
          preLastTime = lastTime
          Thread.sleep(1000)
        }
      }

      //calculate the hit rate
      println(s"######################################## Writing Hitrate ########################################")
      val outHitRate = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("HitRate.txt", true)))
      val iteratorH = hitMiss.entrySet().iterator()
      var hitCount = 0
      var totalSize = 0
      while(iteratorH.hasNext) {
        val pair2 = iteratorH.next()
        val list = pair2.getValue
        totalSize = totalSize + list.size
        for(i <- 0 until list.size) {
          if(list.get(i) == true){
            hitCount = hitCount + 1
          }
        }
      }
      val hitRate = 1.0 * hitCount / totalSize
      var hitRateRst = ""
      val jobName = java.lang.String.valueOf(System.getProperty("CMU_APP_NAME","Test Job Name"))
      hitRateRst = hitRateRst + index + "\t" + jobName + "\t" + hitRate + "\n"
      outHitRate.write(hitRateRst)
      println(s"######################################## Finish Writing Hitrate ########################################")
      out.close()
      outHitRate.close()
      out_record.close()
    }
  }
}
