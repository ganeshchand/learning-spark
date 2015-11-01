package com.gc.learning.scala

import java.io.{IOException, FileNotFoundException}
import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
 * Created by ganeshchand on 11/1/15.
 * Download data from www.ssa.gov/OACT/babynames/limits.html
 */
case class BabyName(state: String, sex: String, year: Int, name: String, count: Int)

object BabyNamesAnalysisUsingScala {

  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: BabyNamesAnalysisUsingScala <path/to/file>")
      System.exit(1)
    }
    var bufferListOfNames = new ListBuffer[BabyName]()


    def parseLineUsingCaseClass(line: String): BabyName = {
      val parts = line.split(",")
      BabyName(parts(0), parts(1), parts(2).toInt, parts(3), parts(4).toInt) // return BabyName object
    }

    try {
      for (line <- Source.fromFile(args(0)).getLines()) {
        val nameDataObject = parseLineUsingCaseClass(line)
        bufferListOfNames += nameDataObject
      }
    } catch {
      case ex: FileNotFoundException => {
        println(s"Couldn't find the file at ${args(0)}")
        System.exit(1)
      }
      case ex: IOException => println(s"Exception occurred while reading a file")

    }

    val namesList = bufferListOfNames.toList
    println(s"Total Baby Name: ${namesList.size}")

    val maleNamesList = namesList.filter(b => b.sex == "M")
    val femaleNamesList = namesList.filter(b => b.sex == "F")

    println(s"Total Female Baby Name: ${femaleNamesList.size}")
    println(s"Total Male Baby Name: ${maleNamesList.size}")

    val maxNameCount = namesList.map(b => b.count).max
    val mostPopularName = namesList.filter(b => b.count == maxNameCount).map(b => b.name)
    println(s"Most Popular Baby Name: ${mostPopularName(0)}")

    val maxnameCountFemale = femaleNamesList.map(b => b.count).max

    val mostPopularFemaleName = femaleNamesList.filter(b => b.count == maxnameCountFemale).map(b => b.name)
    println(s"Most Popular Baby Name: ${mostPopularFemaleName(0)}")
  }

}
