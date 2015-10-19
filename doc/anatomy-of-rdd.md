#Topics

##What is RDD?

###Resilient Distributed Dataset
*   A big collection of data with following properties
*   Immutable: Once created, it never changes. Adv: Parallelism
*   Distributed
*   Lazily evaluated
*   Type inferred
*   Cacheable

##Immutable and Distributed


##Partitions

*   Logical Division of data
*   Derived from Hadoop Map/Reduce (Splits)
*   All Input, Intermediate and Output data will be represented as partitions
*   Partitions are basic unit of parallelisms
*   RDD data is just a collection of partitions

####Notes:
*   In HDFS, chunks are the physical division of data. For each chunk of the data, you can have one or more part partitions. (By default, it is one chunk => one partition)
*   Spark does partitions using Hadoop api (e.g. InputFormat)


##Partitions and Immutability
*   All partitions are immutable
*   Every transformation generates new partition
*   Partition immutability driven by underneath storage like HDFS
*   Partition immutability allows for fault tolerance

##Accessing Partitions

*   We can access partition together rather than single row at a time
*   mapPartitions API of RDD allows us to do that
*   Perform partition-wise operations

##Laziness
##Caching
##Extending Spark API
