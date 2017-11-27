package fr.inria.zenith

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._


object DataGenerator {

  def func(tup: Array[Double]): Array[Float] = {
    val tstemp = tup
    val ts  = new Array[Float](tstemp.length)
    for (i <- 1 until ts.length)
      ts(i) =  ts(i-1) + tstemp(i-1).toFloat
    return ts
  }

  def main(args: Array[String]): Unit = {

    // Parameters
    val options = new Options()
    options.addOption("numFiles", true, "")
    options.addOption("tsNum", true, "")
    options.addOption("outputPath", true, "")
    options.addOption("tsSize", true, "")
    options.addOption("csv", true, "")


    val clParser = new BasicParser()
    val cmd = clParser.parse(options, args)

    val numFiles = cmd.getOptionValue("numFiles", "10").toInt
    val tsNum = cmd.getOptionValue("tsNum", "10").toInt
    val outputPath = cmd.getOptionValue("outputPath", "test.data")
    val tsSize = cmd.getOptionValue("tsSize", "10").toInt
    val csv = cmd.getOptionValue("csv", "false").toBoolean


    // Execution
    val conf: SparkConf = new SparkConf().setAppName("Data generator")
                            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Array[Float]], classOf[scala.collection.immutable.Range] ))

    val sc: SparkContext = new SparkContext(conf)

    val rdd = normalVectorRDD(sc, tsNum, tsSize, numFiles)
    val finalRdd = rdd.map(tup => func(tup.toArray)).zipWithUniqueId().map(tuple => (tuple._2, tuple._1))

    if (csv) {
      finalRdd
        .map(a => a._1.toString + "," + a._2.mkString(","))
        .saveAsTextFile(outputPath)
    }
    else {
      finalRdd.saveAsObjectFile(outputPath)
    }
    System.out.println(finalRdd.toDebugString)
    sc.stop()

  }
}