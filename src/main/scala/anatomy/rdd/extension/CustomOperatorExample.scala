package anatomy.rdd.extension

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by ganeshchand on 10/18/15.
 */
object CustomOperatorExample {

  class SalesRecordCustomFunctions(rdd: RDD[SalesRecord]) {

    def totalSales = rdd.map(record => record.itemValue).sum()
  }

  object SalesRecordCustomFunctions{
    implicit def addSalesRecordCustomFunctions(rdd: RDD[SalesRecord]) = new SalesRecordCustomFunctions(rdd)
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "Extending Spark - Add Domain Specific Function to RDD")
    val rdd = sc.textFile(args(1))

    val salesRecordRDD = rdd.map(line => {
      val values = line.split(",")
      new SalesRecord(values(0),values(1),values(2),values(3).toDouble)
    })

   salesRecordRDD.collect.toList.foreach(println)

    println(s"Build-in RDD Operator - Sum:  ${salesRecordRDD.map(_.itemValue).sum()}")

    //Using Domain Specific Language Custom RDD Operator
    import SalesRecordCustomFunctions.addSalesRecordCustomFunctions
    println(s"Custom RDD Operator - totalSales:  ${salesRecordRDD.totalSales}")
  }

}
