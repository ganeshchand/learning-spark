import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ganeshchand on 10/13/15.
 */
object HelloWorld {
  def main(args: Array[String]) {
    val string = "Hello, World"

    val conf = new SparkConf().setAppName("Hello-World")
    .setMaster("local[*]")
    .set("spark.executor.memory", "2g")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(string)

    println(rdd.collect.mkString(""))
    sc.stop()

  }
}
