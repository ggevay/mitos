package gg

import java.nio.file.{FileSystems, Files}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object ClickCountNoJoinNoAction {

  def main(args: Array[String]): Unit = {

    val pref = args(0) + "/"
    val days = args(1).toInt

    val outDir = pref + "out_nojoin/spark/"
    Files.createDirectories(FileSystems.getDefault.getPath(outDir))

    val conf = new SparkConf().setAppName("ClickCountNoJoin")
      //.setMaster("local[10]") // TODO: vigyazat !!!!!!!!!!!
    val sc = new SparkContext(conf)

    println("=== sc.defaultParallelism: " + sc.defaultParallelism)

    var yesterdayCounts: RDD[(Int, Int)] = null

    var allDiffs = sc.emptyRDD[Int]

    for (day <- 1 to days) {
      println("### Day " + day)

      //val file: RDD[String] = sc.textFile(pref + "in/clickLog_" + day, sc.defaultParallelism)

//      val file: RDD[String] = sc.textFile(pref + "in/clickLog_" + day)
//        .repartition(sc.defaultParallelism)
//      val visits: RDD[Int] = file.map(ln => ln.toInt)
//      val counts = visits.map(v => (v, 1)).reduceByKey(_+_).persist(StorageLevel.MEMORY_ONLY_SER)

      val file: RDD[String] = sc.textFile(pref + "in/clickLog_" + day, sc.defaultParallelism)
      val counts = file.map(ln => (ln.toInt, 1)).reduceByKey(_+_, sc.defaultParallelism).persist(StorageLevel.MEMORY_ONLY_SER)

      if (day != 1) {

        val s = counts.fullOuterJoin(yesterdayCounts).map {case (k, (l, r)) =>
          math.abs(l.getOrElse(0) - r.getOrElse(0))
        }//.fold(0)(_+_)

        //reflect.io.File(outDir + "diff_" + day).writeAll(s.toString)

        //yesterdayCounts.unpersist()

        allDiffs = allDiffs union s
      }

      yesterdayCounts = counts
    }

    val cnt = allDiffs.count() // This will trigger all the RDDs in the loop
    println(s"Final count: ${cnt}")
  }
}

