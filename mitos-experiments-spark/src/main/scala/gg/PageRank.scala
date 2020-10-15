package gg

import java.nio.file.{FileSystems, Files}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object PageRank {

  def main(args: Array[String]): Unit = {

    val pref = args(0) + "/"
    val days = args(1).toInt

    val outDir = pref + "out/spark/"
    Files.createDirectories(FileSystems.getDefault.getPath(outDir))

    val d = 0.85
    val epsilon = 0.0001

    val conf = new SparkConf().setAppName("PageRank")
      //.setMaster("local[2]") // TODO: vigyazat !!!!!!!!!!!
    val sc = new SparkContext(conf)
    //sc.setCheckpointDir("hdfs://cloud-11:44000/user/ggevay/spark_checkpoints")

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)

    var yesterdayPR: RDD[(Int, Double)] = null

    for (day <- 1 to days) {
      println("### Day " + day)

      //val file: RDD[String] = sc.textFile(pref + "/input/" + day, sc.defaultParallelism)
      val file: RDD[String] = sc.textFile(pref + "/input/" + day)
        .repartition(sc.defaultParallelism)
      val edges0: RDD[(Int, Int)] = file.map(ln => {
        val sp = ln.split("\t")
        (sp(0).toInt, sp(1).toInt)
      }).persist()

      val pages: RDD[Int] = edges0.flatMap{case (a,b) => Seq(a,b)}.distinct()
        .persist()

      val loopEdges: RDD[(Int, Int)] = pages.map(x => (x,x))

      val edges: RDD[(Int, Int)] = edges0 union loopEdges

      edges0.unpersist()

      val edgesWithDeg: RDD[(Int, (Int, Int))] = // (src,(dst,srcdeg))
        edges.map{case (a,b) => (a,1)}.reduceByKey(_ + _).join(edges).map{case (k,(v,w)) => (k,(w,v))}
          .persist()

      val initWeight: Double = 1.0d / pages.count()

      var PR: RDD[(Int, Double)] = pages.map(x => (x, initWeight))
        .persist()

      pages.unpersist()

      var innerDelta: Double = -1
      do {

        val msgs: RDD[(Int, Double)] =
          PR.join(edgesWithDeg).map{case (src, (value, (dst, srcdeg))) => (dst, value / srcdeg)}.reduceByKey(_ + _)

        val jump = (1-d) * initWeight
        val newPR: RDD[(Int, Double)] = msgs.map{case (id, v) => (id, d*v+jump)}
          .persist()

        innerDelta = PR.join(newPR).map{case (k,(oldval, newval)) => scala.math.abs(oldval-newval)}.sum()

        PR.unpersist()
        PR = newPR

      } while (innerDelta > epsilon)

      edgesWithDeg.unpersist()


      if (day != 1) {

        val s = PR.fullOuterJoin(yesterdayPR).map {case (k, (l, r)) =>
          math.abs(l.getOrElse(0.0) - r.getOrElse(0.0))
        }.sum()

        val rounded = scala.math.round(s * 100d) / 100d
        reflect.io.File(outDir + "diff_" + day).writeAll(rounded.toString + "\n")

        yesterdayPR.unpersist()
      }

      yesterdayPR = PR
    }
  }
}

