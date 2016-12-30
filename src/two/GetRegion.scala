package two

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
import java.sql.Date
import java.sql.Connection
import java.sql.PreparedStatement

object GetRegion {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("getoracle")
    val sc = new SparkContext(conf)

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
        DriverManager.getConnection("jdbc:oracle:thin:@10.1.35.19:1521:orcl", "lxbt", "lxbt")
      },
      "SELECT * FROM dic_region WHERE rownum >= ? AND rownum <= ?",
      0, 7748, 1,
      r => (r.getString(1), r.getString(2), r.getString(3), r.getString(4),
        r.getString(5), r.getString(6), r.getString(7)))
    //    rdd.collect().foreach { bk => println(bk.a1 + ":::" + bk.a2 + ":::" + bk.a3) }
    //    rdd.foreachPartition (dealData)
    rdd.coalesce(1, true).saveAsTextFile("file:///C:/Users/BuleSky/Desktop/dicRegion")
    sc.stop()
  }

  case class Bak(old_qcode: String,
    old_qname: String,
    old_jdcode: String,
    old_jdname: String,
    old_jwcode: String,
    old_jwname: String,
    id: String)

  case class region(
    id: String, //	编号
    name: String, //	名称
    parent_id: String, //父级字典项id
    order_no: String, //	显示顺序
    status: String, //状态
    levels: String, //	级别
    list_num: String //列表号
    )

  def dealData(bak: Iterator[Bak]): Unit = {
    //    bak.foreach { bk => println(bk.a1 + ":::" + bk.a2 + ":::" + bk.a3) }

    var conn: Connection = null

    var ps: PreparedStatement = null

    val sql = "insert into "

    println(sql)

    try {

      Class.forName("oracle.jdbc.driver.OracleDriver")

      DriverManager.getConnection("jdbc:oracle:thin:@10.1.35.19:1521:orcl", "lxbt", "lxbt")

      ps = conn.prepareStatement(sql)

      ps.executeUpdate()

    } catch {

      case e: Exception => e.printStackTrace

    } finally {

      if (ps != null) {

        ps.close()

      }

      if (conn != null) {

        conn.close()

      }

    }

  }

}