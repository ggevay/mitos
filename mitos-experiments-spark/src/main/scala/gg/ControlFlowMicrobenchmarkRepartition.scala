package gg

import org.apache.spark._

object ControlFlowMicrobenchmarkRepartition {

  def main(args: Array[String]): Unit = {

    val numSteps = args(0).toInt
    val numElements = args(1).toInt

    val conf = new SparkConf().setAppName("ControlFlowMicrobenchmark")
      //.setMaster("local[2]") // TODO: vigyazat !!!!!!!!!!!
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir("hdfs://cloud-11:44000/user/ggevay/spark_checkpoints")

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)

    var coll = sc.parallelize(1 to numElements)

//    for (i <- 1 to numSteps) {
//      println("### Step " + i)
//      coll = coll.map(x => x + 1)
//        .repartition(sc.defaultParallelism)
//
//      coll.cache()
//
//      val emptied = coll
//        .filter(_ => false)
//        .repartition(1)
//      emptied.count()
//    }
//
//    println(coll.count())

    for (i <- 1 to numSteps) {
      println("### Step " + i)

      val onePart = coll.repartition(1)

      onePart.cache()
      onePart.count()

      coll = onePart.repartition(sc.defaultParallelism)
    }

    println(coll.count())
  }
}

