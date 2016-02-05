package com.gc.learning.spark.sql.dataframe

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

/**
  * Created by ganeshchand on 2/4/16.
  */
object DataFrameCustomSchema {
  def main(args: Array[String]) {

    val sc = new SparkContext("local[*]", "Spark DataFrame Custom Schema")
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel(logLevel = "ERROR")
    println(s"Running on Spark Version: ${sc.version}")


    // You can read and parse JSON to DataFrame directly from file
    // Schema is inferred

    println("Using JSON input file")

    val personJsonFilePath = "/Users/ganeshchand/gh/gc/spark/learning-spark/data/dataframe/person.json"
    val personDF = sqlContext.read.json(personJsonFilePath)

    // read.json() doesn't load the data by iteself because data frames are evaluated lazily. But, it does trigger schema reference

    personDF.printSchema()
    println(personDF.show(3))

    // You can also read JSON data from RDD[String] object
    // Schema is inferred

    println("Using RDD[String]")

    val personJsonString =
      """
        {"id": 1,"first_name": "Barack","last_name": "Obama","age": 53,"sex": "male"}
      """ :: Nil // :: Nil makes it a List[String]

    val personStrDF = sqlContext.read.json(sc.parallelize(personJsonString)) // parallelize expects a collection

    personStrDF.printSchema()

    personStrDF.show(1)

    // Specifying your own schema
    // You can improve performance by having Spark to avoid the scan

    val personSchema = (new StructType())
      .add("id", IntegerType, false)
      .add("age", LongType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("sex", StringType)

    val personStrDFCustomSchema = sqlContext.read.schema(personSchema).json(sc.parallelize(personJsonString))

    println("Applying Custom Schema")

    personStrDFCustomSchema.printSchema()
    personStrDFCustomSchema.show(1)

    // Type Casting

    println("Data Frame Type Casting")

    val jsonGithubSimpleString =
      """
         {"id": "2614896652", "type": "CreateEvent","created_at": "2015-03-01T00:00:00Z" }""" :: Nil

    // Let's look at the default behaviour


    val ghubDefaultDf = sqlContext.read.json(sc.parallelize(jsonGithubSimpleString))
    ghubDefaultDf.printSchema()
    // created_at is treated as String
    ghubDefaultDf.show(1) // notice created_at column values are displayed as a String
    // we can cast it as part of select statement

    import sqlContext.implicits._

    ghubDefaultDf.select($"id", $"type", $"created_at".cast(TimestampType)).foreach(println) // notice that created_at is formatted


    // Let's define our own Schema with TimestampType

    // Let's create sample GitHub logs with some messy data for created_at field

    val githubMessyLogString =

        """{"id": 2614896652, "type": "CreateEvent","created_at": "2015-03-01T00:00:00Z"}""" :: // => all field have values
        """{"id": 2614896652, "type": "CreateEvent","created_at": 2015-03-01T00:00:00Z}""" :: // created_at is not string=> all field have values
        """{"id": 2614896652, "type": "CreateEvent","created_at": null }""" :: // null => created_at value is null
        """{"id": 2614896652, "type": "CreateEvent","created_at": 2015-03-01 }""" :: // all null
        """{"id": 2614896652, "type": "CreateEvent","created_at": "" }""" :: // blank => created_at is null
        //"""{"id": null, "type": "CreateEvent","created_at": "null" }""" :: // "null ID"  => results in error
        """{"id": null, "type": "CreateEvent","created_at":  2015-03-01T00:00:00Z}""" :: // "null ID" and created_at is blank => all null
        Nil

    val gitHubMessyLogSchema = (new StructType).add("id", LongType, false).add("type", StringType).add("created_at", TimestampType)
  // id can't be null. We will reject these rows

    val githubMessyLogDF = sqlContext.read.schema(gitHubMessyLogSchema).json(sc.parallelize(githubMessyLogString))

    githubMessyLogDF.printSchema()
    githubMessyLogDF.show  // let's show all

    /**
      * The data type you specified above as part of defining schema is the result of type coercion performed by JSON parser
      * and it has nothing to do with DataFrame's type casting functionality as you saw in df.select()...
      * Type coercions implemented in parser are somewhat limited and in some cases not obvious.
      *
      * Example:
      * In the above example, when created_at is not a string value in JSON, the entire row is consider incorrect and is dropped
      * and it sets all fields to nulls.
      *
      * Therefore, the best option is to explitly provide schema forcing StringType for all untrusted fields to avoid
      * extra RDD scan, and then cast those columns to desired types.
      *
      *
      */

    // Fixing null values problems

    // dealing with Messy data - We know that created_at field can be String, empty, null, timestamp, etc. So,
    // we are going to treat it as String

    val githubMessyLogStringTest =

      """{"id": 2614896652, "type": "CreateEvent","created_at": "123456789101112"}""" :: // => all field have values
        """{"id": 2614896652, "type": "CreateEvent","created_at": 234567891011}""" :: // created_at is not string=> all field have values
        Nil

    val gitHubMessyLogSchemaFixed = (new StructType).add("id", LongType, false).add("type", StringType).add("created_at", StringType)

    val githubMessyLogDFFixed = sqlContext.read.schema(gitHubMessyLogSchemaFixed).json(sc.parallelize(githubMessyLogStringTest))
    githubMessyLogDFFixed.printSchema()
    githubMessyLogDFFixed.show

    // the second row is now displayed even thought it wasn't a string value in JSON.


    // Now you can cast as part of DataFrame select expression

    val githubMessyLogDFFixedTypes = githubMessyLogDFFixed.select($"id", $"type", $"created_at".cast(LongType))

    githubMessyLogDFFixedTypes.printSchema()

    githubMessyLogDFFixedTypes.show


    /**
      * There is a difference in how the timestamp is handled by the JSON parser vs catalyst (i.e. Spark SQL)
      * prser cast behaviour: treats integer values as a number of milliseconds.
      * catalyst cast behaviour: treats integer values as a number of seconds.
      * see SPARK-12744
      *
      * e.g. val rdd = sc.parallelize(
      *
      *
      */

    val rdd = sc.parallelize("""{"ts":1452386229}""" :: Nil)
    sqlContext.read.json(rdd).select($"ts".cast(TimestampType)).show // 2016-01-09 16:37:...

    val schema = (new StructType).add("ts", TimestampType)
    sqlContext.read.schema(schema).json(rdd).show // 1970-01-17 11:26:...



    // Handeling Nested Objects


    val nestedJsonStringTest =
      """{"id": 2614896652,
        |"type": "CreateEvent",
        |"created_at": "2015-03-01T00:00:00Z",
        |"actor":{
        | "id": 123,
        | "name": "Jack"}}}""".stripMargin ::Nil

    val nestedJsonRDD = sc.parallelize(nestedJsonStringTest)

    val nestedJsonDF = sqlContext.read.json(nestedJsonRDD)
    nestedJsonDF.printSchema()
    nestedJsonDF.show

    // using Star (*) expansion

    nestedJsonDF.select($"actor.*").show // shows id and name of an actor

    //Accessing individual columns and aliasing column names

    nestedJsonDF.select(
    $"actor.id".as("User ID"),
    $"actor.name".as("User ID")
    ).show


  }



}
