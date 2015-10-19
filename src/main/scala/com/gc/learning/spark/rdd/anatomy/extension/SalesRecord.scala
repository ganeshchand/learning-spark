package com.gc.learning.spark.rdd.anatomy.extension

/**
 * Created by ganeshchand on 10/18/15.
 * Class represents a Sales Record with Transaction ID, Customer ID, Item ID and Amount
 */
class SalesRecord(
                   val transactionId: String,
                   val customerId: String,
                   val itemId: String,
                   val itemValue: Double
                   ) extends Comparable[SalesRecord] with Serializable {

  override def compareTo(that: SalesRecord): Int = {
    this.transactionId.compareTo(that.transactionId)
  }
  override def toString = s"SalesRecord(transactionId=$transactionId, customerId=$customerId, itemId=$itemId, itemValue=$itemValue)"
}
