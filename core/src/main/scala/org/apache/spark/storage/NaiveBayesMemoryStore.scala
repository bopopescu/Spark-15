package org.apache.spark.storage

// scalastyle:off


//import NaiveBayes._
import edu.cmu.sv.naiveBayes._
import libsvm._
import scala.collection.mutable.{Map, LinkedHashMap, ArrayBuffer}
import java.nio.ByteBuffer
import org.apache.spark.Logging
import scala.util.control.Breaks._
import java.util.ArrayList

private[spark] class EnrichedLinkedHashMap[A, B] extends java.util.LinkedHashMap[A, B] with Logging {

	val usage = new LinkedHashMap[A, ArrayBuffer[Long]]()
  val hitMiss = new LinkedHashMap[A, ArrayBuffer[(Boolean, Long)]]() //hit is true
  val lastProb = new LinkedHashMap[A, Double]() //store the last second's probability
  val trainStructure = new ArrayList[ArrayList[java.lang.Double]]()
  val label = new ArrayList[java.lang.Double]()
  val predictProb = new LinkedHashMap[A, Double]()
  var lastEntryAccessTime:Long = 0L

  private def addUsage(a: A) {
    lastEntryAccessTime = System.currentTimeMillis
    val usages = usage.getOrElseUpdate(a, ArrayBuffer[Long]())
    usages += lastEntryAccessTime
  }
  
  def removeUsageEntries(a: A) {
    usage.remove(a)
    lastProb.remove(a)
    predictProb.remove(a)
  }

  private def addHitMiss(a: A, hit:Boolean) {
    lastEntryAccessTime = System.currentTimeMillis
    val hitMisses = hitMiss.getOrElseUpdate(a, ArrayBuffer[(Boolean, Long)]())
    val tuple = (hit, lastEntryAccessTime)
    hitMisses += tuple
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
    var initialProb = 0.0
    if(a.toString().startsWith("rdd")) {
      initialProb = 60.0
    } else {
      initialProb = 40.0
    }
    lastProb.put(a, initialProb)
    predictProb.put(a, initialProb)
    super.put(a, b)
  }
}

private[spark] class NaiveBayesMemoryStore(blockManager: BlockManager, maxMemory: Long)
  extends MemoryStore(blockManager, maxMemory) {

  override val entries = new EnrichedLinkedHashMap[BlockId, MemoryEntry]
  var predictionCount = 0
	
  val algorithm = java.lang.Integer.valueOf(System.getProperty("CMU_ALGORITHM_ENUM","0"))
  var dataset : DataSet = null
  var eva : Evaluation = null

  var numIterations = 10

  //create the bayes classifier.
  if(algorithm == 1) {
    dataset = new DataSet("segment.data")
    eva = new Evaluation(dataset, "NaiveBayes")
    eva.crossValidation(2)
  } 
  
  private val trainingDataGenerator = new CsvGenerator(entries)
  trainingDataGenerator.start

  protected def findBlocksToReplace (
    entries: EnrichedLinkedHashMap[BlockId, MemoryEntry],
    actualFreeMemory: Long,
    space: Long,
    rddToAdd: Option[Int],
    selectedBlocks: ArrayBuffer[BlockId],
    selectedMemory: Long) : Long = {

    if(algorithm == 1) {
      naiveBayesFindBlocksToReplace(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
    } else if (algorithm == 2){
      rlFindBlocksToReplace(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
    } else if (algorithm == 3) {
      svm_FindBlocksToReplace(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
    } else {
      findBlocksToReplaceOriginal(entries, actualFreeMemory, space, rddToAdd, selectedBlocks, selectedMemory)
    }
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
    predictionCount = predictionCount + 1
    val tempMap = new LinkedHashMap[BlockId, Double]
    synchronized {
      entries.synchronized {
        val iterator = entries.usage.toIterator
        while(iterator.hasNext) {
          val (usageBlockId, blockUsage) = iterator.next()
          val predict = eva.predict(Array(blockUsage.size, entries.getNoUsage(usageBlockId).size))
          tempMap.put(usageBlockId, predict)
          logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
              + s" frequency:" + String.valueOf(blockUsage.size)
              + s" block size:" + String.valueOf(entries.getNoUsage(usageBlockId).size)
              + s" predict:" + String.valueOf(predict))
        }
        val tempList = tempMap.toList.sortBy{_._2}
        breakable {
          for(i <- tempList) {
            val tempBlockId = i._1
            selectedBlocks += tempBlockId
            resultSelectedMemory += entries.getNoUsage(tempBlockId).size
            logInfo(s"Choose to drop Block: " + String.valueOf(tempBlockId)
              + s" resultSelectedMemory: " + String.valueOf(resultSelectedMemory)
              + s" freeMemory: " + String.valueOf(actualFreeMemory + resultSelectedMemory)
              + s" space: " + String.valueOf(space))
            if(actualFreeMemory + resultSelectedMemory >= space) {
              break
            }
          }
        }
      }
    }
    
    //   entries.synchronized {
    //     while (actualFreeMemory + resultSelectedMemory < space && entries.usage.toIterator.hasNext) {
    //       var usageIterator = entries.usage.toIterator
    //       if(usageIterator.hasNext) {
    //         val (usageBlockId, blockUsage) = usageIterator.next()
    //         var finalResult = usageBlockId
    //         val lastAccess = blockUsage.last * 1.0 / System.currentTimeMillis()
    //         var predict = eva.predict(Array(blockUsage.size, entries.getNoUsage(usageBlockId).size))
    //         logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
    //           + s" frequency:" + String.valueOf(blockUsage.size)
    //           + s" block size:" + String.valueOf(entries.get(usageBlockId).size)
    //           + s" last access rate:" + String.valueOf(blockUsage.last / System.currentTimeMillis())
    //           + s" predict:" + String.valueOf(predict))

    //         while(usageIterator.hasNext) {
    //           val (usageTempBlockId, tempblockUsage) = usageIterator.next()
    //           val templastAccess = tempblockUsage.last * 1.0 / System.currentTimeMillis()
    //           val tempPredict = eva.predict(Array(tempblockUsage.size, entries.get(usageTempBlockId).size))
    //           logInfo(s"BlockId:" + String.valueOf(usageTempBlockId) 
    //           + s" frequency:" + String.valueOf(tempblockUsage.size)
    //           + s" block size:" + String.valueOf(entries.get(usageTempBlockId).size)
    //           + s" last access rate:" + String.valueOf(templastAccess) 
    //           + s" predict:" + String.valueOf(tempPredict))
    //           if(predict > tempPredict) {
    //             predict = tempPredict
    //             finalResult = usageTempBlockId
    //           }
    //         }
    //         selectedBlocks += usageBlockId
    //         resultSelectedMemory += entries.getNoUsage(usageBlockId).size
    //         logInfo(s"Choose to drop Block: " + String.valueOf(usageBlockId)
    //           + s" timeLine: " + String.valueOf(blockUsage.last)
    //           + s" resultSelectedMemory: " + String.valueOf(resultSelectedMemory)
    //           + s" freeMemory: " + String.valueOf(actualFreeMemory + resultSelectedMemory)
    //           + s" space: " + String.valueOf(space)
    //           + s" access frequency: " + String.valueOf(blockUsage.size));
    //       }
    //     }
    //   }
    if(predictionCount == 10) {
      logInfo(s"retraining!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
      dataset.dataReset(entries.trainStructure, entries.label)
      eva.retrain();
      predictionCount = 0;
    }
    resultSelectedMemory
  }

  private def rlFindBlocksToReplace(
    entries: EnrichedLinkedHashMap[BlockId, MemoryEntry],
    actualFreeMemory: Long,
    space: Long,
    rddToAdd: Option[Int],
    selectedBlocks: ArrayBuffer[BlockId],
    selectedMemory: Long) : Long = {

    logInfo(s"====================ReinforcementFindBlocksToReplace==========")
    
    var resultSelectedMemory = selectedMemory
    predictionCount = predictionCount + 1
    
    synchronized {
      entries.synchronized {
        val tempList = entries.predictProb.toList.sortBy{_._2}
        breakable {
          for(i <- tempList) {
            val tempBlockId = i._1
            selectedBlocks += tempBlockId
            resultSelectedMemory += entries.getNoUsage(tempBlockId).size
            logInfo(s"Choose to drop Block: " + String.valueOf(tempBlockId)
              + s", probability: " + String.valueOf(i._2))
              if(actualFreeMemory + resultSelectedMemory >= space) {
              break
            }
          }
        }
        // val iterator = entries.usage.toIterator
        // while(iterator.hasNext) {
        //   val (usageBlockId, blockUsage) = iterator.next()
        //   val predict = eva.predict(Array(blockUsage.size, entries.getNoUsage(usageBlockId).size))
        //   tempMap.put(usageBlockId, predict)
        //   logInfo(s"BlockId:" + String.valueOf(usageBlockId) 
        //       + s" frequency:" + String.valueOf(blockUsage.size)
        //       + s" block size:" + String.valueOf(entries.get(usageBlockId).size)
        //       + s" predict:" + String.valueOf(predict))
        // }
        // val tempList = tempMap.toList.sortBy{_._2}
        // breakable {
        //   for(i <- tempList) {
        //     val tempBlockId = i._1
        //     selectedBlocks += tempBlockId
        //     resultSelectedMemory += entries.getNoUsage(tempBlockId).size
        //     logInfo(s"Choose to drop Block: " + String.valueOf(tempBlockId)
        //       + s" resultSelectedMemory: " + String.valueOf(resultSelectedMemory)
        //       + s" freeMemory: " + String.valueOf(actualFreeMemory + resultSelectedMemory)
        //       + s" space: " + String.valueOf(space))
        //     if(actualFreeMemory + resultSelectedMemory >= space) {
        //       break
        //     }
        //   }
        // }
      }
    }
    resultSelectedMemory
  }
  
  private def svm_FindBlocksToReplace (
    entries: EnrichedLinkedHashMap[BlockId, MemoryEntry],
    actualFreeMemory: Long,
    space: Long,
    rddToAdd: Option[Int],
    selectedBlocks: ArrayBuffer[BlockId],
    selectedMemory: Long) : Long = {

      // SVM Primitive definition and configuration
      val data : DataSet = new DataSet("segment.data")
      val prob : svm_problem = new svm_problem()
      val param : svm_parameter = new svm_parameter()
      val model : svm_model = new svm_model()

      // SVM Problem or Data Input
      val array_svm_node : ArrayBuffer[Array[svm_node]] = new ArrayBuffer[Array[svm_node]](0)

      for (x <- 0 until data.features.size()) {
          val node_1 : svm_node = new svm_node()
          val node_2 : svm_node = new svm_node()

          node_1.index = 0
          node_1.value = data.features.get(x).get(0)

          node_2.index = 1
          node_2.value = data.features.get(x).get(1)

          val node_col : Array[svm_node] = new Array[svm_node](2)
          node_col(1) = node_1
          node_col(2) = node_2

          array_svm_node += node_col
        }
        
        
      

      val array_svm_label : ArrayBuffer[Double] = new ArrayBuffer[Double]()

      for (x <- 1 until data.labels.size()) {
        array_svm_label += data.labels.get(x)
      }

      prob.x = array_svm_node.toArray
      prob.y = array_svm_label.toArray
      prob.l = data.getNumInstnaces()

      param.svm_type = svm_parameter.C_SVC
      param.kernel_type = svm_parameter.RBF
      param.degree = 3
      param.gamma = 0 // 1/num_features
      param.coef0 = 0
      param.nu = 0.5
      param.cache_size = 100
      param.C = 1
      param.eps = 1e-3
      param.p = 0.1
      param.shrinking = 1
      param.probability = 0
      param.nr_weight = 0
      param.weight_label  = new Array[Int](0)
      param.weight = new Array[Double](0)

      val model2 : svm_model = svm.svm_train(prob, param)
      //svm.svm_save_model()

      var resultSelectedMemory = selectedMemory
      val tempMap = new LinkedHashMap[BlockId, Double]
      synchronized {
        entries.synchronized {
          val iterator = entries.usage.toIterator
          while (iterator.hasNext) {
            val (usageBlockId, blockUsage) = iterator.next()

            val node : svm_node = new svm_node()
            node.index = 0
            node.value = blockUsage.size

            val node2 : svm_node = new svm_node()
            node2.index = 1
            node2.value = entries.getNoUsage(usageBlockId).size

            val node_collection : Array[svm_node] = new Array[svm_node](2)
            node_collection(1) = node
            node_collection(2) = node2

            val totalclass : Array[Double] = new Array[Double](10)
            val prediction : Double = svm.svm_predict_probability(model, node_collection, totalclass)
            tempMap.put(usageBlockId, prediction)
          }

          val tempList = tempMap.toList.sortBy{_._2}
          breakable {
            for (i <- tempList) {
              val tempBlockId = i._1
              selectedBlocks += tempBlockId
              resultSelectedMemory += entries.getNoUsage(tempBlockId).size

              if (actualFreeMemory + resultSelectedMemory >= space) {
                  break
              } 
            }
          }
        }
      }

      return resultSelectedMemory
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
    
    if (space > maxMemory) {
      logInfo(s"Will not store $blockIdToAdd as it is larger than our memory limit")
      return ResultWithDroppedBlocks(success = false, droppedBlocks)
    }

    // Take into account the amount of memory currently occupied by unrolling blocks
    val actualFreeMemory = freeMemory - currentUnrollMemory
    
    logInfo(s"!!!!!!!!!!!!!!!!!!ensureFreeSpace new: actualFreeMemory: "
     + String.valueOf(actualFreeMemory) + s", space: " + String.valueOf(space))


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

// scalastyle:on
