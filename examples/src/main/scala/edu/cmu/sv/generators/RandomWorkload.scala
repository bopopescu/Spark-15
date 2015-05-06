package edu.cmu.sv.generators

import scala.math.random
import org.apache.spark._
import scala.util.Random

abstract class RandomWorkload(randomIterations:Int, interactive: InteractiveWorkload, iterative:IterativeWorkload) {

  def randomWorkload()(implicit spark:SparkContext) {
    for(i <- 1 to randomIterations) {
      if(new Random().nextDouble() >= 0.5)
        interactive.concurrentWorkload()
      else
        iterative.iterativeWorkload()
    }
  }

}
