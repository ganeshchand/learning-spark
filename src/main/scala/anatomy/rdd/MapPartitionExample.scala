package anatomy.rdd

import org.apache.spark.SparkContext

/**
 * Created by ganeshchand on 10/17/15.
 * Demonstrate partition-level operations
 * Goal: Find Max, and Min at each partition level and then across partitions
 */
object MapPartitionExample {

  def main(args: Array[String]) {
    if(args.length < 2 || args.length == 0){
      println("Usage: local[n] <path/to/file>")
      println("\tExample: local[2] developer-certification.md")

      System.exit(-1)
    }
    val sc = new SparkContext(args(0),"map partition example")
    val salesData = sc.textFile(args(1))

    val (min,max)=salesData.mapPartitions(iterator => {
      val (min,max) = iterator.foldLeft((Double.MaxValue,Double.MinValue))((acc,salesRecord) => {
        val itemValue = salesRecord.split(",")(3).toDouble
        (acc._1 min itemValue , acc._2 max itemValue)
      })
      List((min,max)).iterator
    }).reduce((a,b)=> (a._1 min b._1 , a._2 max b._2)) // final computation across the partitions
    println("min = "+min + " max ="+max)

  }


}
