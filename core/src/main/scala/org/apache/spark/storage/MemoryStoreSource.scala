/*
 * 
 */

package org.apache.spark.storage

import com.codahale.metrics.{Gauge,MetricRegistry}

import org.apache.spark.SparkContext
import org.apache.spark.metrics.source.Source

// private[spark] class MemoryStoreSource(val memoryStore: MemoryStore)
//     extends Source {
  // override val metricRegistry = new MetricRegistry()
  // override val sourceName = "BlockManager"

  // metricRegistry.register(MetricRegistry.name("usageSize"), new Gauge[Long] {
  //   override def getValue: Long = {
  //     memoryStore.getUsageSize()
  //   }
  // })

  // metricRegistry.register(MetricRegistry.name("memory", "remainingMem_MB"), new Gauge[Long] {
  //   override def getValue: Long = {
  //     val storageStatusList = blockManager.master.getStorageStatus
  //     val remainingMem = storageStatusList.map(_.memRemaining).sum
  //     remainingMem / 1024 / 1024
  //   }
  // })

  // metricRegistry.register(MetricRegistry.name("memory", "memUsed_MB"), new Gauge[Long] {
  //   override def getValue: Long = {
  //     val storageStatusList = blockManager.master.getStorageStatus
  //     val memUsed = storageStatusList.map(_.memUsed).sum
  //     memUsed / 1024 / 1024
  //   }
  // })

  // metricRegistry.register(MetricRegistry.name("disk", "diskSpaceUsed_MB"), new Gauge[Long] {
  //   override def getValue: Long = {
  //     val storageStatusList = blockManager.master.getStorageStatus
  //     val diskSpaceUsed = storageStatusList.map(_.diskUsed).sum
  //     diskSpaceUsed / 1024 / 1024
  //   }
  // })
// }
