package cn.windylee

import org.apache.spark.sql.SparkSession

object MultiBroadcastTest {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
        .master("local[2]")
      .appName("Multi-Broadcast Test")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = new Array[Int](num)
    for (i <- arr1.indices) {
      arr1(i) = i
    }

    val arr2 = new Array[Int](num)
    for (i <- arr2.indices) {
      arr2(i) = i
    }

    val barr1 = spark.sparkContext.broadcast(arr1)
    val barr2 = spark.sparkContext.broadcast(arr2)

    val observedSize = spark.sparkContext.parallelize(1 to 10, slices)
      .map(_=>(barr1.value.length, barr2.value.length))
    observedSize.collect().foreach(println(_))
    spark.stop()
  }
}
