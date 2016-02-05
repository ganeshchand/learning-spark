package com.gc.learning.spark.app.loganalyzer

/**
 * Created by ganeshchand on 11/1/15.
 */
case class ApacheAccessLog(
                          ipAddress: String,
                          clientID: String,
                          userID: String,
                          dateTimeString: String,
                          method: String,
                          endPoint: String,
                          protocol: String,
                          responseCod3: Int,
                          contentSize: Long
                          ) extends Serializable{

  object ApacheAccessLog {
    // Example Apache log line:
    //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
    val logEntryPattern = """^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\S+)""""r
    val testLogLine = """127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048"""
    println(testLogLine)
//    def parseLogLine(line: String): ApacheAccessLog = {
//
//    }
  }

}
