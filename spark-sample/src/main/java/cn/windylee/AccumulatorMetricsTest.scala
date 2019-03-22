package cn.windylee

import org.apache.spark.sql.SparkSession

object AccumulatorMetricsTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.metrics.conf.*.sink.console.class",
        "org.apache.spark.metrics.sink.ConsoleSink")
      .master("local[6]")
      .getOrCreate()

    val sc = spark.sparkContext
    val acc = sc.longAccumulator("my-long-metric")
    val acc2 = sc.doubleAccumulator("my-double-metric")
    val num = if (args.length > 1) args(0) else 1000000
    val startTime = System.nanoTime
    val source = 1.to(100).toArray
    val accumulatorTest = sc.parallelize(source)
      .foreach(_ => {
        acc.add(1)
        acc2.add(1.1)
      })
    println("Test took %.0f milliseconds".format((System.nanoTime - startTime) / 1E6))
    println("Accumulator values:")
    println("*** Long accumulator (my-long-metric): " + acc.value)
    println("*** Double accumulator (my-double-metric): " + acc2.value)
    spark.stop()
  }
}
