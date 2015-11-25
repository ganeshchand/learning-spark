println("Welcome to Scala Worskheet")


import scala.util.Properties

val master = Properties.envOrElse("MASTER", "local")
//val sparkHome = Properties.get("SPARK_HOME")
val tempDir = Properties.tmpDir
println(tempDir)

val userDir = Properties.userHome
Properties.userName







