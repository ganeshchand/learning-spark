package com.gc.learning.spark.app.textanalysis

import org.apache.spark.SparkContext

/**
 * Created by ganeshchand on 10/27/15.
 */
object SimpleTextAnalysis {
  def main(args: Array[String]) {
    if(args.length < 2){
      println("Usage: SimpleTextAnalysis <master> <path/to/simple.txt>")
      System.exit(-1)
    }

  val sc = new SparkContext(args(0), "SimpleTextAnalysis")
    val lineRDD = sc.textFile(args(1))
    lineRDD.cache()
    println(s"Total Number of Lines found: ${lineRDD.count}")

    val wordsRDD = lineRDD.flatMap(line => line.split(" "))
    println(s"Total Number of words found: ${wordsRDD.count()}")
    println(s"Total Number of unique words found: ${wordsRDD.distinct().count()}")

    //Pair-RDDs
    val wordCountRDD = wordsRDD.map(word => (word, 1)).reduceByKey((x,y) => (x + y))
    println(s"Word Count: ")
    wordCountRDD.collect.foreach(println)


  }
}
