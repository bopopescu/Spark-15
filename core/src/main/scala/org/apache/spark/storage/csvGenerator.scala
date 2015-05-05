package org.apache.spark.storage

import java.util.LinkedList
import java.io._
import java.lang.System
import scala.math.random
import org.apache.spark._
import scala.util.Random
import scala.util.control.Breaks._
import scala.collection.mutable.{LinkedHashMap, ArrayBuffer}
import java.nio.file.{Paths, Files}
import java.io.PrintWriter
import java.util.ArrayList

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
class CsvGenerator(entries:EnrichedLinkedHashMap[BlockId, MemoryEntry]) extends Thread {
 
  final val K = 1.0 // contants for tunning
 
  override def run {

    var count = 0
    var preLastTime = 0L
    var index = 0
    
    println(s"CMU - Usage information written to csv file ")

    try{
      val inHitRate = new BufferedReader(new InputStreamReader(new FileInputStream("HitRate.txt")))
      var str = inHitRate.readLine()
      println(s"######################################## Name: " + getName() + " ########################################")

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
    println(s"CMU - Usage information written to csv file, time: " + String.valueOf(System.currentTimeMillis()))

    val jobName = java.lang.String.valueOf(System.getProperty("CMU_APP_NAME","default name"))
    val useBayes = java.lang.Boolean.valueOf(System.getProperty("CMU_USEBAYES_FLAG","false"))
    //write hit/misses per second per block
    val outHitRate = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("HitRate.txt")))
    //write hitrate per second per block
    val blockHitRate = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("BlockHitRate.txt")))
    //count how many seconds it runs
    var secondsNum = 0


    println(s"CMU - Usage information written to csv file, time: " + String.valueOf(System.currentTimeMillis()))
    
    var algType = "LRU"
    if (useBayes)
      algType = "NaiveBayes"

    outHitRate.write(jobName + "," + algType + "\n")
    outHitRate.flush()

    // out.write("1,1,1,1\n")
    // out_record.write("1,1,1,1\n")
    // out.flush()
    // out_record.flush()

    breakable {
      while(true) {
        entries.synchronized {
          val currTime = System.currentTimeMillis()
          var lastTime = entries.lastEntryAccessTime
          
          //get the total memory access frequency in one second.
          val iteratorU = entries.usage.toIterator
          var sumFreq = 0
          while (iteratorU != null && iteratorU.hasNext) {
            val (blockId, usages) = iteratorU.next()
            sumFreq += usages.size
          }
          
          val iterator = entries.usage.toIterator
          var maxProb = 0.0
          var minProb = -1.0

          blockHitRate.write("time " + secondsNum + " seconds\n")
          blockHitRate.flush()
          while(iterator != null && iterator.hasNext) {
            val (blockId, usages) = iterator.next()
            val freqIndi = usages.size                         //how many times this block been accessed, as f.
            val expectedHitRate = (freqIndi - 1) / freqIndi    //expected maxisum ratio of hitrate, f-1/f.

            val hitList = entries.hitMiss.getOrElse(blockId, new ArrayBuffer[Boolean]())
            var countHit = 0
            for(i <- 0 until hitList.size) {
              if(hitList(i) == true){
                countHit = countHit + 1
              }
            }

            val hitListSize = if(hitList.size != 0) hitList.size else 1
            val realHitRate = 1.0 * countHit / hitListSize
            val hitMissRatio = realHitRate / expectedHitRate 
            val freqRatio = freqIndi / sumFreq                //ratio that shows this block's intense.
            val noUsage = if(entries.getNoUsage(blockId)!=null) entries.getNoUsage(blockId).size else 0
            val blockSize = noUsage
            val size = usages.size
            val ratio = 1.0 * usages(size-1) / currTime
            val newProb = calculateNewProbability(entries.lastProb, blockId, freqRatio, hitMissRatio, blockSize)

            blockHitRate.write(blockId + "," + realHitRate + "\n")
            blockHitRate.flush()
            
            if(maxProb < newProb) {
              maxProb = newProb
            }
            if(minProb == -1.0) {
              minProb = newProb
            } else if(minProb > newProb) {
              minProb = newProb
            }
          }
          
          val iteratorM = entries.usage.toIterator
          while(iteratorM != null && iteratorM.hasNext) {
            val (blockId, usages) = iteratorM.next()
            val noUsage = if(entries.getNoUsage(blockId)!=null) entries.getNoUsage(blockId).size else 0
            val blockSize = noUsage
            val size = usages.size
            val ratio = 1.0 * usages(size-1) / currTime
            val newProb = Math.round((entries.lastProb.get(blockId).get - minProb) / (maxProb - minProb))
            val trainRecord = new ArrayList[java.lang.Double]()
            trainRecord.add(usages.size)
            trainRecord.add(blockSize)
            entries.label.add(newProb)
            entries.trainStructure.add(trainRecord)
            entries.lastProb.put(blockId, newProb)
          }

          val iteratorH = entries.hitMiss.toIterator
          while (iteratorH != null && iteratorH.hasNext) {
            //var strH = "" + secondsNum + "\t"
            var strH = ""
            val (blockId, list) = iteratorH.next()
            
            for(i <- 0 until list.size) {
              if(list(i) == true){
                strH = strH + blockId + ",1\n"
              }
              else{
                strH = strH + blockId + ",0\n"
              }
            }
            outHitRate.write(strH)
            outHitRate.flush()
          }
          entries.hitMiss.clear()

          blockHitRate.write("\n\n")
          blockHitRate.flush()
        }
        Thread.sleep(1000)
        secondsNum = secondsNum + 1
      }
    }
    outHitRate.close()
  }
  
  //reward function
  private def calculateNewProbability(lastProb : LinkedHashMap[BlockId, Double], blockId : BlockId,
    freqRatio : Double, hitMissRatio : Double, blockSize : Long) : Double = {
    val k = K
    val probChange = k * (freqRatio * blockSize) / hitMissRatio
    var newProb = 0.0
    if(lastProb.get(blockId) != None) {
      newProb = lastProb.get(blockId).get + probChange
      lastProb.put(blockId, newProb)
    } else {
      newProb = probChange
      lastProb.put(blockId, newProb)
    }
    newProb
  }
}
