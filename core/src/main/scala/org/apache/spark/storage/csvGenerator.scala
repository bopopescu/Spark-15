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
import java.util.ArrayList

/* 
 * generates an interactive workload
 * execute with: 
 *    ./bin/spark-submit --class edu.cmu.sv.generators.InteractiveWorkloadTest --master local-cluster[2,1,512] ./examples/target/scala-2.10/spark-examples-1.3.0-SNAPSHOT-hadoop1.0.4.jar
 *    you can also use --master local[512]
 */
class CsvGenerator(entries:EnrichedLinkedHashMap[BlockId, MemoryEntry], jobName:String) extends Thread {
 
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

    //write hitrate per second per block
    val outHitRate = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("HitRate.txt")))
    //count how many seconds it runs
    var secondsNum = 0


    println(s"CMU - Usage information written to csv file, time: " + String.valueOf(System.currentTimeMillis()))
    
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
          while(iterator != null && iterator.hasNext) {
            val (blockId, usages) = iterator.next()
            val freqIndi = usages.size                         //how many times this block been accessed, as f.
            val expectedHitRate = (freqIndi - 1) / freqIndi    //expected maxisum ratio of hitrate, f-1/f.
            
            val hitList = entries.hitMiss.get(blockId).get
            var countHit = 0
            for(i <- 0 until hitList.size) {
              if(hitList(i) == true){
                countHit = countHit + 1
              }
            }
            
            val realHitRate = 1.0 * countHit / hitList.size
            val hitMissRatio = realHitRate / expectedHitRate 
            val freqRatio = freqIndi / sumFreq                //ratio that shows this block's intense.
            val blockSize = entries.getNoUsage(blockId).size
            val size = usages.size
            val ratio = 1.0 * usages(size-1) / currTime
            val newProb = calculateNewProbability(entries.lastProb, blockId, freqRatio, hitMissRatio, blockSize)

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
            val (blockId, usages) = iterator.next()
            val blockSize = entries.getNoUsage(blockId).size
            val ratio = 1.0 * usages(size-1) / currTime
            val newProb = (lastProb.get(blockId) - minProb) / (maxProb - minProb)
            val trainRecord = new ArrayList[Double]()
            trainRecord.add(usages.size)
            trainRecord.add(blockSize)
            trainRecord.add(ratio)
            entries.label.add(newProb)
            entries.trainStructure.add(trainRecord)
            lastProb.put(blockId, newProb)
          }

          val iteratorH = entries.hitMiss.toIterator
          while (iteratorH != null && iteratorH.hasNext) {
            //var strH = "" + secondsNum + "\t"
            var strH = ""
            val (blockId, list) = iteratorH.next()
            
            for(i <- 0 until list.size) {
              if(list(i) == true){
                strH = strH + blockId + ",1"
              }
              else{
                strH = strH + blockId + ",0"
              }
            }
            outHitRate.write(strH)
            outHitRate.flush()
          }
          entries.hitMiss.clear()
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
    val newProb = lastProb.get(blockId) + probChange
    lastProb.put(blockId, newProb)
    newProb
  }
}
