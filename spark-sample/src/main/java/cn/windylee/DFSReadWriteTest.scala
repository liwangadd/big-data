package cn.windylee

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.io.Source._

object DFSReadWriteTest {
  private var localFilePath:File=new File(".")
  private var dfsDirPath:String=""

  private val NPARAMS = 2

  private def readFile(filename: String):List[String]={
    val lineIter:Iterator[String] = fromFile(filename).getLines()
    val lineList:List[String] = lineIter.toList
    lineList
  }

  private def printUsage():Unit = {
    val usage = """DFS Read-Write Test
                  |Usage: localFile dfsDir
                  |localFile - (string) local file to use in test
                  |dfsDir - (string) DFS directory for read/write tests""".stripMargin

    println(usage)
  }

  private def parseArgs(args:Array[String]):Unit={
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    var i = 0

    localFilePath = new File(args(i))
    if (!localFilePath.exists) {
      System.err.println(s"Given path (${args(i)}) does not exist")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println(s"Given path (${args(i)}) is not a file")
      printUsage()
      System.exit(1)
    }

    i += 1
    dfsDirPath = args(i)
  }

  def runLocalWordCount(fileContents:List[String]):Int={
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w=>w)
      .mapValues(_.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    println("Performing local word count")
    val fileContents = readFile(localFilePath.toString)
    val localWordCount = runLocalWordCount(fileContents)
    println("Creating SparkSession")

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DFS Read Write Test")
      .getOrCreate()

    println("Writing local file to DFS")
    val dfsFilename = s"$dfsDirPath/dfs_read_write_test"
    val fileRDD = spark.sparkContext.parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsFilename)

    println("Reading file from DFS and running Word Count")
    val readFileRDD = spark.sparkContext.textFile(dfsFilename)
    val dfsWordCount = readFileRDD.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w=>(w, 1))
      .countByKey()
      .values
      .sum
    spark.stop()

    if (localWordCount == dfsWordCount) {
      println(s"Success! Local Word Count $localWordCount and " +
        s"DFS Word Count $dfsWordCount agree.")
    } else {
      println(s"Failure! Local Word Count $localWordCount " +
        s"and DFS Word Count $dfsWordCount disagree.")
    }
  }
}
