package cn.windylee

import org.apache.spark.sql.SparkSession

import scala.util.Random

object SimpleSkewedGroupByTest {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("SimpleSkewedGroupByTest")
      .getOrCreate()

    val numMappers = if (args.length > 0) args(0).toInt else 2
    val numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers
    val ratio = if (args.length > 4) args(4).toInt else 5.0

    val pair1 = spark.sparkContext.parallelize(0 until numMappers, numMappers)
      .flatMap{p=>
        val ranGen = new Random
        val result = new Array[(Int, Array[Byte])](numKVPairs)
        for(i<-0 until numKVPairs){
          val byteArr = new Array[Byte](valSize)
          ranGen.nextBytes(byteArr)
          val offset = ranGen.nextInt(1000) * numReducers
          if(ranGen.nextDouble()<ratio/(numReducers + ratio+1)){
            result(i)=(offset, byteArr)
          }else{
            val key = 1+ranGen.nextInt(numReducers)+offset
            result(i)=(key, byteArr)
          }
        }
        result
      }.cache()
    pair1.count()
    println(s"RESULT: ${pair1.groupByKey(numReducers).count()}")
    spark.stop()
  }
}
