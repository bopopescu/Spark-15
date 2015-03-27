/*
 * CMU-SV Research Project
 */

package org.apache.spark.storage

import scala.language.implicitConversions

import org.scalatest.{FunSuite, BeforeAndAfterEach, Matchers, PrivateMethodTester}
import org.scalatest.mock._
import org.mockito.Mockito._

import org.apache.spark.SparkConf
import org.apache.spark.util._

import java.nio.ByteBuffer

object MemoryStoreSuite {

  final val MAX_MEMORY = 12000  

  case class TestData(blockId:RDDBlockId, initialSize:Int) {
    lazy val data:ByteBuffer = ByteBuffer.wrap(new Array[Byte](initialSize))
    def size:Long = initialSize
  }

  def testData(rddId:Int, splitId:Int, initialSize:Int):TestData = {
    TestData(RDDBlockId(rddId, splitId), initialSize)
  }
}

class MemoryStoreSuite extends FunSuite with Matchers with BeforeAndAfterEach  
  with PrivateMethodTester with ResetSystemProperties {

  import MemoryStoreSuite._  
  
  val tryToPutValue = PrivateMethod[ResultWithDroppedBlocks]('tryToPut)
  var memoryStore:MemoryStore = _  

  override def beforeEach(): Unit = {
    val blockManager = mock(classOf[BlockManager])
    val sparkConf = mock(classOf[SparkConf])
    when(blockManager.conf).thenReturn(sparkConf)
    memoryStore = new MemoryStore(blockManager, MAX_MEMORY)
  }

  override def afterEach(): Unit = {
    memoryStore.clear()
    memoryStore = null
  }

  test("tryToPut should add an item to our data structure") {    
    val a1 = testData(0, 0, 4000)
    val a2 = testData(0, 1, 4000)
    memoryStore invokePrivate tryToPutValue(a1.blockId, a1.data, a1.size, false)
    memoryStore invokePrivate tryToPutValue(a2.blockId, a2.data, a2.size, false)
    assert(memoryStore.contains(a1.blockId), "a1 was not in memory store")
    assert(memoryStore.contains(a2.blockId), "a2 was not in memory store")
    assert(memoryStore.usage.containsKey(a1.blockId), "a1 was not in usage")
    assert(memoryStore.usage.get(a1.blockId).size == 1, "a1 has invalid access count")
    assert(memoryStore.usage.containsKey(a2.blockId), "a2 was not in usage")
    assert(memoryStore.usage.get(a2.blockId).size == 1, "a2 has invalid access count")    
  }

  test("each access should increase the access count in our data structure") {
    val a1 = testData(0, 0, 4000)
    memoryStore invokePrivate tryToPutValue(a1.blockId, a1.data, a1.size, false)
    assert(memoryStore.getSize(a1.blockId) == 4000, "a1 had unexpected size")
    assert(memoryStore.getBytes(a1.blockId) == Some(a1.data))
    assert(memoryStore.getValues(a1.blockId) == Some(null)) //null due to blockmanager mock
    assert(memoryStore.usage.get(a1.blockId).size == 4)
  }

  test("remove") {
    val a1 = testData(0, 0, 4000)    
    memoryStore invokePrivate tryToPutValue(a1.blockId, a1.data, a1.size, false)    
    assert(memoryStore.usage.containsKey(a1.blockId), "a1 not in usage store")
    memoryStore.remove(a1.blockId)
    assert(!memoryStore.usage.containsKey(a1.blockId), "a1 in usage store")
  }
}
