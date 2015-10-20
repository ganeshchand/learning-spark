
###[Spark UI - http://localhost:4040](http://localhost:4040)


##HDFS
###how to add a json file in HDFS
mkdir jsondata
vi jsondata/person.json
hdfs dfs -put jsondata /user/hadoopuser/jsondata


##SPARK
$spark-shell --driver-memory 2G

$spark-shell --driver-class-path/path-to-mysql-jar/mysql-connector-java-5.1.34-bin.jar

Spark Maven Coordinates: http://mvnrepository.com/artifact/org.apache.spark


##Relational Database
*   Construct a JDBC URL: val jdbcURL = "jdbc:mysql://localhost:3306/hadoopdb"
*   Create a connection property object with username and password:
    val prop = new java.util.Properties
    prop.setProperty("user","userName")
    prop.setProperty("password","password")

*   Load DataFrame with JDBC data source(url, tableName, properties)


    val peopleDF = sqlContext.read.jdbc(jdbcURL, "personTable", prop)
    peopleDF.show()
    val first_names = people.select("first_name")
    val below60 = people.filter(people("age") < 60)
    val groupedByGender = people.groupBy("gender")

    val maleOnlyPeopleDF = sqlContext.read.jdbc(jdbcURL, "personTable",
    Array("gender='M'"), prop)

    maleOnlyPeopleDF.show()







