package com.gc.learning.spark.rdd.anatomy.extension

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by ganeshchand on 10/18/15.
  */
object DiscountRDDExample {

  class CustomFunctions(rdd: RDD[SalesRecord]){

    def discount(discountPercentage: Double) = new DiscountRDD(rdd, discountPercentage)
  }

  object CustomFunctions {
    implicit def addCustomFunctions(rdd: RDD[SalesRecord]) = new CustomFunctions(rdd)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "discountRDD")
    val inputRDD = sc.textFile(args(1))

    val salesRecordRDD = inputRDD.map(line => {
      val values = line.split(",")
      new SalesRecord(
        values(0),
        values(1),
        values(2),
        values(3).toDouble)
    })

    println("Sales Record RDD: "+salesRecordRDD.collect().toList)

    import CustomFunctions.addCustomFunctions

    val discountRDD = salesRecordRDD.discount(0.2)

    println("Discount RDD: "+discountRDD.collect().toList)
  }
}
