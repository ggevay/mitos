package gg

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ConnectedComponents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ConnectedComponents")
    conf.set("spark.graphx.pregel.checkpointInterval", "50")
    conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("hdfs://cloud-11:44000/user/ggevay/spark_checkpoints")

    val graph = GraphLoader.edgeListFile(sc, args(0), numEdgePartitions = sc.defaultParallelism)
    val cc = graph.connectedComponents().vertices
    cc.saveAsTextFile(args(1))
  }
}

