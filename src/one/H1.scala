package one

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf

object H1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("first").setMaster("local[1]")
    val sc = new SparkContext(sparkConf)
    println(sc)
    val rdd1 = sc.textFile("file:///C:/Users/BuleSky/Desktop/a.txt")
    val rdd2 = rdd1.flatMap(_.split(" ")).map { x => (x, 1) }
    rdd2.foreach(p => println(">>> key=" + p._1 + ", value=" + p._2))
    val pth1 = "hdfs://me:9000/data/b.txt"
    val pth2 = "file:///C:/Users/BuleSky/Desktop/b.txt"
    rdd2.repartition(1).saveAsTextFile(pth2)
    sc.stop()
  }
}