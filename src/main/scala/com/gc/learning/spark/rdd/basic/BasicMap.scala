package com.gc.learning.spark.rdd.basic

import org.apache.spark.SparkContext

/**
 * Created by ganeshchand on 10/29/15.
 * Illustrates simple map() in spark
 */
object BasicMap {
  def main(args: Array[String]) {
    val master = args.length match {
      case n: Int if (n > 0) => args(0)
      case _ => "local[*]"
    }

//    val filePath = args.length match {
//      case n: Int if (n > 1) => args(1)
//      case _ => getClass.getResource("simpletext.txt").getPath
//    }

val sc = new SparkContext(master, "BasicMap")
    val lineRDD = sc.textFile("/Users/ganeshchand/gh/projects/gc/spark/learning-spark/src/main/resources/simpletext.txt")
    println(s"Number of Lines ${lineRDD.count}")
  }
}
