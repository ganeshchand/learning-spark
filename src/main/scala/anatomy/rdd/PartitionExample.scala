package anatomy.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ganeshchand on 10/13/15.
 */
object PartitionExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Partition Example")
    .setMaster("local[2]") // 2  threads
    .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    val inputFile = getClass.getClassLoader.getResource("data/anatomy-of-rdd/sales.csv").getPath
    if(new java.io.File(inputFile).exists()){

//      val rdd = sc.textFile(inputFile)
      /**
       * The below code is the spark implementation of sc.textFile(inputfile)
       * in Java: className.class(TextInputFormat)
       * This demonstrates that spark internally uses Hadoop api
       * When dealing with text file, internally it uses TextInputFormat
       */
      val rdd = sc.hadoopFile(inputFile,
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text],
      sc.defaultMinPartitions  // default minimum partitions
      ).map(pair => pair._2.toString)  // The pair consists of LongWritable as key and Text
      //as the value. Spark ignores the LongWritable (which is the record offset) and gets only the value (which is the line)

      println("Total lines in the input file: "+rdd.count)
      println("Number of RDD partitions: "+rdd.partitions.length)  // access rdd partition

      /**
       * defaultMinPartitions is based on parallel executors (threads). On local mode,
       * the default is 1. However, when you setMaster("local[2]"), the default partition
       * changes to 2
       */

    }else{
      println("Input File couldn't be found")
      System.exit(-1)
    }


  }
}
