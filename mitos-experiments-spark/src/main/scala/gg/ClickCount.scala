package gg

import java.nio.file.{FileSystems, Files}

import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object ClickCount {

  def main(args: Array[String]): Unit = {

    val pref = args(0) + "/"
    val days = args(1).toInt

    val outDir = pref + "out/spark/"
    Files.createDirectories(FileSystems.getDefault.getPath(outDir))

    val conf = new SparkConf().setAppName("ClickCountNoJoin")
      //.setMaster("local[10]") // TODO: vigyazat !!!!!!!!!!!
    val sc = new SparkContext(conf)

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)

    val pageAttributes0 =
      sc.textFile(pref + "in/pageAttributes.tsv", sc.defaultParallelism)
      .map(ln => {
        val arr = ln.split('\t')
        (arr(0).toInt, arr(1).toInt)
      })

    val pageAttributes = pageAttributes0
      .partitionBy(defaultPartitioner(pageAttributes0))
      .persist(StorageLevel.MEMORY_ONLY_SER)

    var yesterdayCounts: RDD[(Int, Int)] = null

    for (day <- 1 to days) {
      println("### Day " + day)

      //val file: RDD[String] = sc.textFile(pref + "in/clickLog_" + day, sc.defaultParallelism)

//      val file: RDD[String] = sc.textFile(pref + "in/clickLog_" + day)
//        .repartition(sc.defaultParallelism)
//      val visits: RDD[Int] = file.map(ln => ln.toInt)
//      val counts = visits.map(v => (v, 1)).reduceByKey(_+_).persist(StorageLevel.MEMORY_ONLY_SER)

      val visits: RDD[(Int, Unit)] = sc.textFile(pref + "in/clickLog_" + day, sc.defaultParallelism).map(ln => (ln.toInt, ()))
      val visitsFiltered = visits.join(pageAttributes).filter(x => x._2._2 == 0).map(x => x._1)
      val counts = visitsFiltered.map(v => (v, 1)).reduceByKey(_+_, sc.defaultParallelism).persist(StorageLevel.MEMORY_ONLY_SER)

      if (day != 1) {

        val s = counts.fullOuterJoin(yesterdayCounts).map {case (k, (l, r)) =>
          math.abs(l.getOrElse(0) - r.getOrElse(0))
        }.fold(0)(_+_)

        reflect.io.File(outDir + "diff_" + day).writeAll(s.toString)

        yesterdayCounts.unpersist()
      }

      yesterdayCounts = counts
    }
  }
}

