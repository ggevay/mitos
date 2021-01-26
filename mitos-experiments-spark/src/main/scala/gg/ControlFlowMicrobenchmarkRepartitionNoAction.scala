package gg

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object ControlFlowMicrobenchmarkRepartitionNoAction {

  def main(args: Array[String]): Unit = {

    val numSteps = args(0).toInt
    val numElements = args(1).toInt

    val conf = new SparkConf().setAppName("ControlFlowMicrobenchmark")
      //.setMaster("local[2]") // TODO: vigyazat !!!!!!!!!!!
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir("hdfs://cloud-11:44000/user/ggevay/spark_checkpoints")

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)

    var coll = sc.parallelize(1 to numElements)

//    val allColl = new ListBuffer[RDD[Int]]
//
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
//      allColl += emptied
//    }
//
//    val u = sc.union(allColl)
//    println(u.count())

    val allOnePart = new ListBuffer[RDD[Int]]

    for (i <- 1 to numSteps) {
      println("### Step " + i)

      val onePart = coll.repartition(1)

      onePart.cache()
      allOnePart += onePart

      coll = onePart.repartition(sc.defaultParallelism)
    }

    val u = sc.union(allOnePart)
    println(u.count())
  }
}

