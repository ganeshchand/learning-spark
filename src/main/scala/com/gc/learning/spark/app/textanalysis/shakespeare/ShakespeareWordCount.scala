package com.gc.learning.spark.app.textanalysis.shakespeare

import org.apache.spark.SparkContext

/**
 * Created by ganeshchand on 10/27/15.
 */
object ShakespeareWordCount {
  def main(args: Array[String]) {
    if(args.length < 2){
      println("Usage: ShakespeareWordCount <master> <path/to/shakespear.txt>")
      System.exit(-1)
    }

  val sc = new SparkContext(args(0), "ShakespearWordCount")
    val lineRDD = sc.textFile(args(1))

    val lineCount = lineRDD.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x,y) => (x + y)).collect()
    println(s"Total Words = $lineCount")
  }
}
