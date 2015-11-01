package com.gc.learning.spark.sql.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


/**
 * Created by ganeshchand on 10/19/15.
 * Reads person.json file and saves the output
 */
object ReadAndSaveJson {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("Usage: ReadAndSaveJson <master_node> <input-dir> <output-dir>")
      System.exit(-1)
    }
    val sc = new SparkContext(args(0), "Spark DataFrame Json example")
    //    import SQLContext.
    val sqlContext = new SQLContext(sc)

    val person = sqlContext.read.json(args(1))
    person.registerTempTable("person")

    println("Printing Schema")
    println(person.printSchema())
    val sixtyPlus = sqlContext.sql("select * from person where age > 60")

    sixtyPlus.collect.foreach(println)
    println(s"Saving SixtyPlus result at $args(2)/OutputOfReadAndSaveJson")
    sixtyPlus.toJSON.saveAsTextFile(args(2) + "/OutputOfReadAndSaveJson")

    sc.stop()

  }
}
