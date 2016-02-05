package com.gc.learning.spark.sql.dataframe

import java.io.{IOException, File}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source._

/**
  * Created by ganeshchand on 2/2/16.
  *
  * This example reads GitHub log file in JSON format. The purpose of this program is to count
  * the number of Push Event to find out how many Git Pushes are being done in a Company and order the count in descending order.
  *
  * For the purpose of testing and avoid dependency on the data availability, if you do not specify
  * the file path as the first argument, then the program will use the sample Git event.
  */
object GithubLogs {


  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("GitHub Log Push Counter")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    args.length match {
      case 1 => runGithubEmployeePushAnalyzer(sqlContext.read.json(args(0)))
      case 0 => runGithubEmployeePushAnalyzer(sqlContext.read.json(sc.parallelize(getSampleGithubLogJsonString)))
    }

    def runGithubEmployeePushAnalyzer(df: DataFrame) = {
      val ghLogPushTypes = df.filter("type = 'PushEvent'")
      println(s"All GitHub events: ${df.count}")
      println(s"Only Push Events: ${ghLogPushTypes.count}")

      val pushes = df.filter("type = 'PushEvent'")
      val grouped = pushes.groupBy("actor.login").count()
      val ordered = grouped.orderBy(grouped("count").desc)


      val employees = Set() ++ (
        for {
        //        line <- fromFile(args(1)).getLines()
          line <- fromFile("/Users/ganeshchand/gh/gc/spark/learning-spark/data/sia/ghEmployees.txt").getLines()
        } yield line.trim()
        )
      val bcEmployees = sc.broadcast(employees)

      // UDF to filter ordered Data Frame for employees that are part of the employees set only

      import sqlContext.implicits._

      val isEmp: (String => Boolean) = user => bcEmployees.value.contains(user)

      val isEmpSqlFunc = sqlContext.udf.register("Set Contains Employee UDF", isEmp)

      val filtered = ordered.filter(isEmpSqlFunc($"login"))

      val outputPath = "/tmp/spark-output/ghlog/"
      if (new File(outputPath).exists()) {
        println(s"Output Path - $outputPath already exists. Deleting...")
        deleteDirectory(outputPath)
      }
      filtered.coalesce(1) // to produce single file
      filtered.write.json(outputPath)
      println(s"Output is written at $outputPath")
    }

    def deleteDirectory(path: String) = {
      try {
        FileUtils.deleteDirectory(new File(path))
      } catch {
        case ioe: IOException => s"IOException - Error deleting $path"
        case e: Exception => s"Exception occurred deleting $path"
      }
    }

    def getSampleGithubLogJsonString = {
      """{
"id": "2614896652",
"type": "CreateEvent",
"actor": {
  "id": 739622,
  "login": "treydock",
  "gravatar_id": "",
  "url": "https://api.github.com/users/treydock",
  "avatar_url": "https://avatars.githubusercontent.com/u/739622?"
},
"repo": {
  "id": 23934080,
  "name": "Early-Modern-OCR/emop-dashboard",
  "url": "https://api.github.com/repos/Early-Modern-OCR/emop-dashboard"
},
"payload": {
  "ref": "development",
  "ref_type": "branch",
  "master_branch": "master",
  "description": "",
  "pusher_type": "user"
},
"public": true,
"created_at": "2015-03-01T00:00:00Z",
"org": {
  "id": 10965476,
  "login": "Early-Modern-OCR",
  "gravatar_id": "",
  "url": "https://api.github.com/orgs/Early-Modern-OCR",
  "avatar_url": "https://avatars.githubusercontent.com/u/10965476?"
}
},

      """ :: Nil
    }


  }
}
