package two

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ConverData {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("getoracle")
    val sc = new SparkContext(conf)
    val data=sc.textFile("file:///C:/Users/BuleSky/Desktop/part-00000")
    data.repartition(1).saveAsTextFile("file:///C:/Users/BuleSky/Desktop/bak");
    sc.stop()
  }
}