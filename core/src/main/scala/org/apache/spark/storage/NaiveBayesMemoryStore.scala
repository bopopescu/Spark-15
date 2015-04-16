package org.apache.spark.storage

import NaiveBayes._

private[spark] class EnrichedLinkedHashMap[A, B] extends java.util.LinkedHashMap[A, B] {

	import scala.collection.mutable.{LinkedHashMap, ArrayBuffer}

	private val usage = new LinkedHashMap[BlockId, ArrayBuffer[Long]]()
  private val hitMiss = new LinkedHashMap[BlockId, ArrayBuffer[Boolean]]() //hit is true
  private val lastEntryAccessTime = new ArrayBuffer[Long]()

	override def get(a: Any): B = {
		super.get(a)
	}

	override def put(a:A, b:B):B = {
		super.put(a, b)
	}
}

private[spark] class NaiveBayesMemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends MemoryStore(blockManager, maxMemory) {

  override val entries = new EnrichedLinkedHashMap[BlockId, MemoryEntry]
	
  var dataset : DataSet = null
	var eva : Evaluation = null

  // lastEntryAccessTime.add(0)

  val useBayes = java.lang.Boolean.valueOf(System.getProperty("CMU_USEBAYES_FLAG","false"))

  if(useBayes) {
    dataset = new DataSet("segment.data")

    eva = new Evaluation(dataset, "NaiveBayes")
    eva.crossValidation(2)
  }

  //create the bayse classifier.
  // for (String path : dataPaths) {

  if(useBayes) {
    dataset = new DataSet("segment.data")

    eva = new Evaluation(dataset, "NaiveBayes")
    eva.crossValidation(2)
  }

  // val testonly = Array(4000.0)
  // val prediction = test.predict(testonly)
  // print mean and standard deviation of accuracy
  // System.out.println("Dataset:" + path + ", mean and standard deviation of accuracy:" + eva.getAccMean() + "," + eva.getAccStd());
  // Ensure only one thread is putting, and if necessary, dropping blocks at any given time

  // private val trainingDataGenerator = new csvGenerator(usage, hitMiss, entries, lastEntryAccessTime)
  // trainingDataGenerator.start

  // private def addHitMiss(blockId:BlockId, hit:Boolean) {
  //   val ll = hitMiss.get(blockId)
  //   if(ll == null) {
  //     val nll = new LinkedList[Boolean]()
  //     nll.add(hit)
  //     hitMiss.put(blockId, nll)
  //   }
  //   else
  //     ll.add(hit)
  // }

  // private def getEntry(blockId:BlockId) = {
  //   val v = entries.get(blockId)
  //   if(v == null)
  //     addHitMiss(blockId, false)
  //   else
  //     addHitMiss(blockId, true)
  //   v
  // }

//   private val trainingDataGenerator = new csvGenerator(usage, hitMiss, entries, lastEntryAccessTime)
// +  trainingDataGenerator.start


//   +
// +  /**
// +  *Define the ways to choose which blcok to swap out
// +  */
// +  //question: the parameters passed in are val?
// +  private def findBlocksToReplace (
// +    entries: LinkedHashMap[BlockId, MemoryEntry],
// +    actualFreeMemory: Long,
// +    space: Long,
// +    rddToAdd: Option[Int],
// +    selectedBlocks: ArrayBuffer[BlockId],
// +    selectedMemory: Long) : Long = {
// +    var resultSelectedMemory = selectedMemory
// +    synchronized {
// +      entries.synchronized {
// +        val cmuEntries = entries.entrySet()
// +        val iterator = cmuEntries.iterator()
// +        while (actualFreeMemory + selectedMemory < space && iterator.hasNext) {
// +          val pair = iterator.next()
// +          val blockId = pair.getKey
// +          if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
// +            logInfo(s"########################## blockId is $blockId ##############")
// +            selectedBlocks += blockId
// +            resultSelectedMemory += pair.getValue.size
// +            logInfo(s"Block: " + String.valueOf(blockId)
// +                + s" timeLine: " + String.valueOf(usage.get(blockId).get(0))
// +                + s" access frequency: " + String.valueOf(usage.get(blockId).size()));
// +          }
// +        }
// +      }
// +      logInfo(s"----------------------------test for bayse------------------------")
// +
// +      var usageEntries = usage.entrySet()
// +      var usageIterator = usageEntries.iterator()
// +      if(usageIterator.hasNext) {
// +        var usagePair = usageIterator.next()
// +        var usageBlockId = usagePair.getKey
// +        var predict = eva.predict(Array(usage.get(usageBlockId).size(), entries.get(usageBlockId).size))
// +        logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
// +          + s" frequency:" + String.valueOf(usage.get(usageBlockId).size())
// +          + s" block size:" + String.valueOf(entries.get(usageBlockId).size)
// +          + s" last access rate:" + String.valueOf(usage.get(usageBlockId).getLast() / System.currentTimeMillis()) 
// +          + s" predict:" + String.valueOf(predict))
// +        while(usageIterator.hasNext) {
// +          usagePair = usageIterator.next()
// +          var usageTempBlockId = usagePair.getKey
// +          var tempPredict = eva.predict(Array(usage.get(usageTempBlockId).size(), entries.get(usageBlockId).size))
// +          logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
// +          + s" frequency:" + String.valueOf(usage.get(usageBlockId).size())
// +          + s" block size:" + String.valueOf(entries.get(usageBlockId).size)
// +          + s" last access rate:" + String.valueOf((usage.get(usageBlockId).getLast() * 1.0) / (System.currentTimeMillis() * 1.0)) 
// +          + s" predict:" + String.valueOf(predict))
// +          if(predict > tempPredict) {
// +            predict = tempPredict
// +            usageBlockId = usageTempBlockId
// +          }
// +        }
// +        logInfo(s"Choose to drop Block: " + String.valueOf(usageBlockId)
// +          + s" timeLine: " + String.valueOf(usage.get(usageBlockId).getLast())
// +          + s" access frequency: " + String.valueOf(usage.get(usageBlockId).size()));
// +      }
// +
// +      logInfo(s"----------------------------test end------------------------")
// +    // TODO: utilize usage structure
// +    
// +    }
// +    resultSelectedMemory
// +  }
// +  
// +  
// +  private def findBlocksToReplaceOriginal (
// +    entries: LinkedHashMap[BlockId, MemoryEntry],
// +    actualFreeMemory: Long,
// +    space: Long,
// +    rddToAdd: Option[Int],
// +    selectedBlocks: ArrayBuffer[BlockId],
// +    selectedMemory: Long) : Long = {
// +  // This is synchronized to ensure that the set of entries is not changed
// +  // (because of getValue or getBytes) while traversing the iterator, as that
// +  // can lead to exceptions.
// +    var resultSelectedMemory = selectedMemory
// +    entries.synchronized {
// +      val iterator = entries.entrySet().iterator()
// +      while (actualFreeMemory + selectedMemory < space && iterator.hasNext) {
// +        val pair = iterator.next()
// +        val blockId = pair.getKey
// +        if (rddToAdd.isEmpty || rddToAdd != getRddId(blockId)) {
// +          selectedBlocks += blockId
// +          resultSelectedMemory += pair.getValue.size
// +        }
// +      }
// +    }
// +    resultSelectedMemory
// +  }
// +


//    if(useBayes) {
// -      // (because of getValue or getBytes) while traversing the iterator, as that		+        selectedMemory = findBlocksToReplace(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
// -      // can lead to exceptions.		+      } else {
// -      entries.synchronized {		+        selectedMemory = findBlocksToReplaceOriginal(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
// -        		+      }

//   +   * generate the csv file of usage info for naive bayesian training.
// +   */
// +  def writeUsageInfo() {
// +    logInfo(s"CMU - Usage information written to csv file ")
// +    val out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("usageHistory.csv", true)))
// +    val iterator = usage.entrySet().iterator()
// +    while (iterator.hasNext) {
// +      var str = ""
// +      val pair = iterator.next()
// +      val blockId = pair.getKey
// +      val freq = pair.getValue.size
// +      str = str + blockId + "," + freq + "\n"
// +      out.write(str)
// +    }
// +    out.close()
// +  }
// +
// +  /**
// +   * get the size of usage
// +   */
// +  def getUsageSize(): Long = {
// +    var usageSize = usage.size
// +    usageSize
// +  }

}