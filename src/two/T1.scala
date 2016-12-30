package two

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object T1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("getoracle")
    val sc = new SparkContext(conf)
    
    val r=sc.textFile("file:///C:/Users/BuleSky/Desktop/bc.txt", 2);
    
    r.collect().foreach { a => println(a) }
    println(r)
    
    r.saveAsTextFile("file:///C:/Users/BuleSky/Desktop/bb")
    
    sc.stop()

  }
}