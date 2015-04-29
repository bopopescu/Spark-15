package org.apache.spark.storage

import NaiveBayes._
import scala.collection.mutable.{LinkedHashMap, ArrayBuffer}
import java.nio.ByteBuffer
import org.apache.spark.Logging

private[spark] class EnrichedLinkedHashMap[A, B] extends java.util.LinkedHashMap[A, B] with Logging {

	val usage = new LinkedHashMap[A, ArrayBuffer[Long]]()
  val hitMiss = new LinkedHashMap[A, ArrayBuffer[Boolean]]() //hit is true
  val lastProb = new LinkedHashMap[A, Int]() //store the last second's probability
  val trainStructure = new ArrayBuffer[ArrayBuffer[Any]]()
  var lastEntryAccessTime:Long = 0L

  private def addUsage(a: A) {
    lastEntryAccessTime = System.currentTimeMillis
    val usages = usage.getOrElseUpdate(a, ArrayBuffer[Long]())
    usages += lastEntryAccessTime
  }
  
  def removeUsageEntries(a: A) {
    usage.remove(a)
  }

  private def addHitMiss(a: A, hit:Boolean) {
    val hitMisses = hitMiss.getOrElseUpdate(a, ArrayBuffer[Boolean]())
    hitMisses += hit
  }

  def getNoUsage(a: A): B = super.get(a)

	override def get(a: Any): B = {
    val b = super.get(a)
    addUsage(a.asInstanceOf[A])
    addHitMiss(a.asInstanceOf[A], b != null)
    b
	}

	override def put(a:A, b:B):B = {
    addUsage(a)
    addHitMiss(a, false)
    super.put(a, b)
	}
}

private[spark] class NaiveBayesMemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends MemoryStore(blockManager, maxMemory) {

  override val entries = new EnrichedLinkedHashMap[BlockId, MemoryEntry]
  //TODO: design a data transportation structure for training.
	
  val useBayes = java.lang.Boolean.valueOf(System.getProperty("CMU_USEBAYES_FLAG","false"))
  logInfo(s"==============useBayes===========" + String.valueOf(useBayes))
  var dataset : DataSet = null
  var eva : Evaluation = null
  
  val jobName = java.lang.String.valueOf(System.getProperty("CMU_APP_NAME","Iterative Workload"))
  var selectedInputFile = "segment1.data"
  
  if(jobName == "Iterative Workload")
    selectedInputFile = "segment1.data"
  else if (jobName =="Interactive Workload")
    selectedInputFile = "segment2.data"
  else if(jobName == "Random Workload")
    selectedInputFile = "segment3.data"

  //create the bayes classifier.
  if(useBayes) {
    dataset = new DataSet(selectedInputFile)
    eva = new Evaluation(dataset, "NaiveBayes")
    eva.crossValidation(2)
  }
  
  
  private val trainingDataGenerator = new CsvGenerator(entries, jobName)
  trainingDataGenerator.start

  protected def findBlocksToReplace (
    entries: EnrichedLinkedHashMap[BlockId, MemoryEntry],
    actualFreeMemory: Long,
    space: Long,
    rddToAdd: Option[Int],
    selectedBlocks: ArrayBuffer[BlockId],
    selectedMemory: Long) : Long = {

    if(useBayes)
      naiveBayesFindBlocksToReplace(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
    else
      findBlocksToReplaceOriginal(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
  }

  private def naiveBayesFindBlocksToReplace(
    entries: EnrichedLinkedHashMap[BlockId, MemoryEntry],
    actualFreeMemory: Long,
    space: Long,
    rddToAdd: Option[Int],
    selectedBlocks: ArrayBuffer[BlockId],
    selectedMemory: Long) : Long = {

    logInfo(s"====================naiveBayesFindBlocksToReplace==========")
    
    var resultSelectedMemory = selectedMemory
    synchronized {
      entries.synchronized {
        while (actualFreeMemory + resultSelectedMemory < space && entries.usage.toIterator.hasNext) {
          var usageIterator = entries.usage.toIterator
          if(usageIterator.hasNext) {
            var (usageBlockId, blockUsage) = usageIterator.next()        
            val lastAccess = blockUsage.last * 1.0 / System.currentTimeMillis()
            var predict = eva.predict(Array(blockUsage.size, entries.getNoUsage(usageBlockId).size, lastAccess))
            logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
              + s" frequency:" + String.valueOf(blockUsage.size)
              + s" block size:" + String.valueOf(entries.get(usageBlockId).size)
              + s" last access rate:" + String.valueOf(blockUsage.last / System.currentTimeMillis())
              + s" predict:" + String.valueOf(predict))

            while(usageIterator.hasNext) {
              var (usageTempBlockId, blockUsage) = usageIterator.next()
              var tempPredict = eva.predict(Array(blockUsage.size, entries.get(usageBlockId).size, lastAccess))
              logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
              + s" frequency:" + String.valueOf(blockUsage.size)
              + s" block size:" + String.valueOf(entries.get(usageBlockId).size)
              + s" last access rate:" + String.valueOf((blockUsage.last * 1.0) / (System.currentTimeMillis() * 1.0)) 
              + s" predict:" + String.valueOf(predict))
              if(predict > tempPredict) {
                predict = tempPredict
                usageBlockId = usageTempBlockId
              }
            }
            selectedBlocks += usageBlockId
            resultSelectedMemory += entries.getNoUsage(usageBlockId).size
            logInfo(s"Choose to drop Block: " + String.valueOf(usageBlockId)
              + s" timeLine: " + String.valueOf(blockUsage.last)
              + s" access frequency: " + String.valueOf(blockUsage.size));
          }
        }
      }
    }
    resultSelectedMemory
  }

  private def findBlocksToReplaceOriginal (
    entries: EnrichedLinkedHashMap[BlockId, MemoryEntry],
    actualFreeMemory: Long,
    space: Long,
    rddToAdd: Option[Int],
    selectedBlocks: ArrayBuffer[BlockId],
    selectedMemory: Long) : Long = {
  // This is synchronized to ensure that the set of entries is not changed
  // (because of getValue or getBytes) while traversing the iterator, as that
  // can lead to exceptions.
    var resultSelectedMemory = selectedMemory
    entries.synchronized {
      val iterator = entries.entrySet().iterator()
      while (actualFreeMemory + resultSelectedMemory < space && iterator.hasNext) {
        val pair = iterator.next()
        val blockId = pair.getKey
        if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
          selectedBlocks += blockId
          resultSelectedMemory += pair.getValue.size
        }
      }
    }
    resultSelectedMemory
  }

  protected override def ensureFreeSpace(
      blockIdToAdd: BlockId,
      space: Long): ResultWithDroppedBlocks = {

    val droppedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    logInfo(s"ensureFreeSpace new!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    if (space > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }

    // Take into account the amount of memory currently occupied by unrolling blocks
    val actualFreeMemory = freeMemory - currentUnrollMemory

    if (actualFreeMemory < space) {
      val rddToAdd = getRddId(blockIdToAdd)
      val selectedBlocks = new ArrayBuffer[BlockId]
      var selectedMemory = 0L      
      
      findBlocksToReplace(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)      

      if (actualFreeMemory + selectedMemory >= space) {
        logInfo(s"${selectedBlocks.size} blocks selected for dropping")
        for (blockId <- selectedBlocks) {
          val entry = entries.synchronized { entries.get(blockId) }
          // This should never be null as only one thread should be dropping
          // blocks and removing entries. However the check is still here for
          // future safety.
          if (entry != null) {
            val data = if (entry.deserialized) {
              Left(entry.value.asInstanceOf[Array[Any]])
            } else {
              Right(entry.value.asInstanceOf[ByteBuffer].duplicate())
            }
            val droppedBlockStatus = blockManager.dropFromMemory(blockId, data)
            droppedBlockStatus.foreach { status => droppedBlocks += ((blockId, status)) }
          }
        }
        return ResultWithDroppedBlocks(success = true, droppedBlocks)
      } else {
        logInfo(s"Will not store $blockIdToAdd as it would require dropping another block " +
          "from the same RDD")
        return ResultWithDroppedBlocks(success = false, droppedBlocks)
      }
    }
    ResultWithDroppedBlocks(success = true, droppedBlocks)
  }

  override def remove(blockId: BlockId): Boolean = {
    logInfo(s"======================remove=======================")
    entries.synchronized {
      entries.removeUsageEntries(blockId)
      
      super.remove(blockId)
    }
  }
}