package com.gc.learning.spark.sql
import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by ganeshchand on 1/18/16.
  */
object PostgreSQL {
  def main(args: Array[String]) {
    val databaseType = "postgresql"
    val jdbcUsername = "postgres"
    val jdbcPassword = "postgres"
    val jdbcHostname = "localhost"
    val jdbcPort = 5432
    val jdbcDatabase ="postgres"
    val jdbcUrl = s"jdbc:$databaseType://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"
    val connectionProperties = new java.util.Properties()
    var connection: Connection = null
    try {
      Class.forName("org.postgresql.Driver");
      connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
      println("PostgreSQL JDBC Driver registered and connection is created.")
    } catch {
      case cnf: ClassNotFoundException => println("PostgreSQL JDBC driver not found!")
      case e: Exception => e.printStackTrace()
    }
    connection.isClosed()
    val sc = new SparkContext("local[*]", "Read Data From PostgreSQL")
    //    import SQLContext.
    val sqlContext = new SQLContext(sc)
    val weatherDF = sqlContext.read.jdbc(jdbcUrl, "weather", connectionProperties)
    weatherDF.printSchema()
    weatherDF.registerTempTable("weather")
    val weatherDataCount = sqlContext.sql("select count(*) from weather")
    println(s"Total Rows: ${weatherDataCount.collectAsList().get(0).getLong(0)}")
  }
}
