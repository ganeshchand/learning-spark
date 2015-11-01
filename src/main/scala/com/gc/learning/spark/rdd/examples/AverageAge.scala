package com.gc.learning.spark.rdd.examples

import org.apache.spark.SparkContext

/**
 * Created by ganeshchand on 10/20/15.
 */
object AverageAge {
  def main(args: Array[String]) {
    if(args.length < 3){
      println("""
                 Usage: AverageAge local[*] <input_dir> <output_dir>

                 <input_dir> is the directory that contains sales.csv file
                 <output_dir> is the directory where the output will be saved.
              """)
      System.exit(-1)
    }
    val sc = new SparkContext(args(0), "Spark Aggregate Example")
    sc.setLogLevel(logLevel = "WARN")
    val baseRDD = sc.textFile(args(1))
    val peopleByAgeRDD = baseRDD.map(line => line.split(",")).map(t => (t(2).toInt, t))
//
//    val aggRDD = peopleByAgeRDD.aggregate((0, 0))(
//      (acc, value) => (acc._1 + value, acc._2 + 1),
//      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
//
////    aggRDD.collect.foreach(println())
//
//    sc.makeRDD()
//    sc.parallelize()




  }
}
