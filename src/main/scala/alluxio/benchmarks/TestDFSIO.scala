/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.benchmarks

import java.net.URI

import org.apache.commons.cli._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._

import scala.io.Source
import scala.util.Random

object TestDFSIO {

  private val MB = 1024L * 1024L
  private val TEST_DIR = "TestDFSIO/"
  // the sub directory for storing the control files
  private val CONTROL_DIR = "control/"
  // the sub directory for the data
  private val DATA_DIR = "data/"
  private val FILE_PREFIX = "part-"
  private val TIMES_PREFIX = "-time-"

  private def getOptions(): Options = {
    OptionBuilder.isRequired()
    OptionBuilder.hasArg()
    OptionBuilder.withArgName("PARTITIONS")
    OptionBuilder.withDescription("number of partitions")
    val partitionsOption = OptionBuilder.create("p")

    OptionBuilder.isRequired()
    OptionBuilder.hasArg()
    OptionBuilder.withArgName("PARTITION_SIZE")
    OptionBuilder.withDescription("size of each partition, in MB")
    val sizeOption = OptionBuilder.create("s")

    OptionBuilder.isRequired()
    OptionBuilder.hasArg()
    OptionBuilder.withArgName("BASE_DIR")
    OptionBuilder.withDescription("directory for the base directory")
    val basePathOption = OptionBuilder.create("b")

    OptionBuilder.isRequired()
    OptionBuilder.hasArg()
    OptionBuilder.withArgName("OPERATIONS")
    OptionBuilder.withDescription("operations to run ('w' for writes, 'r' for reads")
    val operationsOption = OptionBuilder.create("o")

    OptionBuilder.isRequired()
    OptionBuilder.hasArg()
    OptionBuilder.withArgName("TIMES")
    OptionBuilder.withDescription("How many times you want run")
    val timesOption = OptionBuilder.create("t")

    new Options()
      .addOption(partitionsOption)
      .addOption(sizeOption)
      .addOption(basePathOption)
      .addOption(operationsOption)
      .addOption(timesOption)
  }

  private def printUsage(): Unit = {
    val formatter = new HelpFormatter()
    formatter.printHelp("TestDFSIO", "", getOptions(), "", true)
  }

  def main(args: Array[String]) {
    var partitions = 0L
    var partitionSize = 0L
    var basePath = ""
    var operations = ""
    var times = 0L
    try {
      val parser = new BasicParser()
      val cmdLine = parser.parse(getOptions(), args)
      if (cmdLine == null) {
        printUsage()
        return
      }
      partitions = cmdLine.getOptionValue("p").toLong
      partitionSize = cmdLine.getOptionValue("s").toLong
      basePath = cmdLine.getOptionValue("b")
      operations = cmdLine.getOptionValue("o")
      times = cmdLine.getOptionValue("t").toLong
    } catch {
      case e: Exception => {
        printUsage()
        return
      }
    }

    println("partitions: " + partitions)
    println("partitionSize: " + partitionSize)
    println("basePath: " + basePath)
    basePath = basePath + TEST_DIR
    println("new basePath: " + basePath)
    println("operations: " + operations)
    println("times: " + times)

    val conf = new SparkConf().setAppName("DFSIO")
    val sc = new SparkContext(conf)

    val tester = new TestDFSIO(partitions, partitionSize, basePath)

    tester.runOperations(sc, operations, times)

    Thread.sleep(10000)

    sc.stop()
  }
}

/**
  * Runs benchmark for a sequence of operations
  * partitions:     number of partitions/tasks to run
  * partitionSize:  how much data in bytes to be read/written
  * basePath:       base path for test data
  */
class TestDFSIO(private val partitions: Long,
                private val partitionSize: Long,
                private val basePath: String) extends java.io.Serializable {
  private val controlPath = basePath + TestDFSIO.CONTROL_DIR
  private val dataPath = basePath + TestDFSIO.DATA_DIR

  /**
   * Stores benchmark result for one IO operation
   * name:      name of the operation
   * seconds:   time taken to complete the operation, in seconds
   * taskData:  array of task results in the format of (number of bytes processed, milliseconds taken)
   */
  class Result(val name: String, val seconds: Double, val taskData: Array[(Long, Long)]) {
    override def toString(): String = {
      val totalBytes = taskData.map(_._1).sum.toDouble
      val strBytes = "%.2f".format(totalBytes)
      val strSeconds = "%.2f".format(seconds)
      val strMbPerSec = "%.2f".format(totalBytes.toDouble / (seconds * TestDFSIO.MB))
      val strGbPerSec = "%.2f".format(totalBytes.toDouble / (seconds * TestDFSIO.MB * 1024))

      // MB / s, for each task
      val taskThroughputs = taskData.map(x => 1000.0 * x._1.toDouble / (x._2.toDouble * TestDFSIO.MB))
      val strTaskThroughputs = taskThroughputs.map("%.2f".format(_))
      val strTaskMbPerSec = "%.2f".format(taskThroughputs.sum)
      val strTaskGbPerSec = "%.2f".format(taskThroughputs.sum / 1024.0)

      var s = "---------- " + name + " ----------\n"
      s += "Job Throughput: (" + strBytes + " bytes / " + strSeconds + "  s) = " + strMbPerSec + " MB/s = " + strGbPerSec + " GB/s" + "\n"
      s += "Task Throughputs [MB/s]: " + strTaskThroughputs.mkString(", ") + "\n"
      s += "Aggregate Task Throughput: " + strTaskMbPerSec + " MB/s = " + strTaskGbPerSec + " GB/s"
      s
    }
  }

  def runOperations(sc: SparkContext, operations: String, times: Long): Unit = {
    operations.map(_ match {
      case 'w' | 'W' => runWrite(sc, times)
      case 'r' | 'R' => runRead(sc)
      case _ => new Result("", 1.0, Array())
    }).foreach(println _)
  }

  private def generateBuffer(): Array[Byte] = {
    var words: Array[String] = null
    val file = Source.fromInputStream(getClass.getResourceAsStream("/words.txt"))
    try {
      words = file.getLines().flatMap(_.split(" ")).toArray
    } finally {
      file.close()
    }

    val buf = new StringBuilder()
    val random = new Random()
    var lineLength = 0

    while (buf.length < TestDFSIO.MB) {
      if (!buf.isEmpty) {
        if (lineLength >= 1024 && (TestDFSIO.MB - buf.length) >= 1024) {
          buf += '\n'
          lineLength = 0
        } else {
          buf += ' '
          lineLength += 1
        }
      }
      val nextWord = words(random.nextInt(words.length))
      buf ++= nextWord
      lineLength += nextWord.length()
    }

    val source = buf.toString()
    // Removes 1 character to add the new line character last.
    val rawLine = buf.substring(0, TestDFSIO.MB.toInt - 1)
    val line = rawLine.concat("\n")
    line.getBytes()
  }

  private def generateControlFiles(sc: SparkContext, times: Long): Unit = {
    val writeRdd = sc.parallelize(0 until partitions.toInt, partitions.toInt).map(x => {
      var t = 0
      while (t < times){
      val dataRelativeFile = TestDFSIO.DATA_DIR + TestDFSIO.FILE_PREFIX + x + TestDFSIO.TIMES_PREFIX + t
      val controlFile = controlPath + TestDFSIO.FILE_PREFIX + x + TestDFSIO.TIMES_PREFIX + t
      val outstream = FileSystem.get(new URI(controlFile), new Configuration())
        .create(new Path(controlFile))
      try {
        outstream.write(dataRelativeFile.getBytes())
      } finally {
        outstream.close()
      }
      t += 1
      }
      1
    })

    writeRdd.sum()
  }

  private def runWrite(sc: SparkContext, times: Long): Result = {
    val fs = FileSystem.get(new URI(basePath), new Configuration())
    if (fs.exists(new Path(basePath))) {
      println("path exists, deleting existing folder: " + basePath)
      fs.delete(new Path(basePath), true)
    } else {
      println("path does not exist: " + basePath)
      fs.mkdirs(new Path(basePath))
    }

    Thread.sleep(2000)

    generateControlFiles(sc, times)

    val data = generateBuffer()

    val writeRdd = sc.textFile(controlPath, partitions.toInt).map(x => {

      val startMs = System.currentTimeMillis()

          val dataFile = basePath + x.trim()
          var i = partitionSize
          val outstream = FileSystem.get(new URI(dataFile), new Configuration()).create(new Path(dataFile))
          try {
            while (i > 0) {
              outstream.write(data)
              i -= 1
            }
          } finally {
            outstream.close()
          }
      val stopMs = System.currentTimeMillis()
      val durationMs = stopMs - startMs
      (partitionSize * TestDFSIO.MB, durationMs)

    })

    val startMs = System.currentTimeMillis()
    val taskData = writeRdd.collect()
    val stopMs = System.currentTimeMillis()

    new Result("WRITE", (stopMs - startMs) / 1000.0, taskData)
  }

  private def runRead(sc: SparkContext): Result = {

    val readRdd = sc.textFile(controlPath, partitions.toInt).map(x => {
      val data = new Array[Byte](TestDFSIO.MB.toInt)

      val startMs = System.currentTimeMillis()
      var totalBytesRead = 0L

          val dataFile = basePath + x.trim()
          val instream = FileSystem.get(new URI(dataFile), new Configuration()).open(new Path(dataFile))
          try {
            var bytesRead = instream.read(data)
            while (bytesRead != -1) {
              totalBytesRead += bytesRead
              bytesRead = instream.read(data)
            }
          } finally {
            instream.close()
          }

      val stopMs = System.currentTimeMillis()
      val durationMs = stopMs - startMs
      (totalBytesRead, durationMs)
    })

    val startMs = System.currentTimeMillis()
    val taskData = readRdd.collect()
    val stopMs = System.currentTimeMillis()

    new Result("READ", (stopMs - startMs) / 1000.0, taskData)
  }
}
