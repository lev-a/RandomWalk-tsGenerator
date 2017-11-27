package fr.inria.zenith

import org.apache.commons.cli.{BasicParser, Options}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs._


object DataGenerator {
/*
  def generateRandomTS(tsSize: Int, initVal: Float): Array[Float] = {
    val ts : Array [Float] = new Array[Float](tsSize)
    ts(0) = initVal
    for (i <- 1 until ts.length)
      ts(i) =  ts(i-1) + scala.util.Random.nextGaussian().toFloat
    return ts
  }
*/
  def func(tup: Array[Double]): Array[Float] = {
    val tstemp = tup
    val ts  = new Array[Float](tstemp.length)
    //  ts(0) = initVal
    for (i <- 1 until ts.length)
      ts(i) =  ts(i-1) + tstemp(i-1).toFloat
    return ts
  }

  def main(args: Array[String]): Unit = {

    // Parameters
    val options = new Options()
    options.addOption("numFiles", true, "")
   // options.addOption("tuplesPerFile", true, "")
    options.addOption("tsNum", true, "")
    options.addOption("outputPath", true, "")
    options.addOption("tsSize", true, "")


    val clParser = new BasicParser()
    val cmd = clParser.parse(options, args)

    val numFiles = cmd.getOptionValue("numFiles", "10").toInt
   // val tuplesPerFile = cmd.getOptionValue("tuplesPerFile", "100").toInt
    val tsNum = cmd.getOptionValue("tsNum", "10").toInt
    val outputPath = cmd.getOptionValue("outputPath", "test.data")
    val tsSize = cmd.getOptionValue("tsSize", "10").toInt



    // Execution
    val conf: SparkConf = new SparkConf().setAppName("Data generator")
                                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Array[Float]], classOf[scala.collection.immutable.Range] ))

    val sc: SparkContext = new SparkContext(conf)
//val tsNum  = 1000 //TODO parameter

    val rdd = normalVectorRDD(sc, tsNum, tsSize, numFiles)

    val finalRdd = rdd.map(tup => func(tup.toArray)).zipWithUniqueId().map(tuple => (tuple._2, tuple._1))


    finalRdd.saveAsObjectFile(outputPath)
    System.out.println(finalRdd.toDebugString)
    sc.stop()

    /*

    val dummyRDD = sc.parallelize(0 until numFiles, numFiles)

    val tuples = dummyRDD.flatMap(fileNum => {
      (0 until tuplesPerFile).map(tupleNum => generateRandomTS (tsSize, 0.0f))
    }).zipWithUniqueId().map(tuple => (tuple._2, tuple._1))

    tuples.saveAsObjectFile(outputPath)
*/
    //tuples.mapValues(_.toList).foreach(println(_))
  }

}