package two

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import java.sql.DriverManager
import java.sql.Date
import java.sql.Connection
import java.sql.PreparedStatement

object GetData {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("getoracle")
    val sc = new SparkContext(conf)

    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
        DriverManager.getConnection("jdbc:oracle:thin:@10.1.35.19:1521:orcl", "lxbt", "lxbt")
      },
      "SELECT * FROM lxbt_person_info_hist_demo_bak WHERE rownum >= ? AND rownum <= ?",
      0, 207999, 1,
      r => (r.getString(22), r.getString(23) , r.getString(24)))
    //    rdd.collect().foreach { bk => println(bk.a1 + ":::" + bk.a2 + ":::" + bk.a3) }
    //    rdd.foreachPartition (dealData)
    rdd.coalesce(1, true).saveAsTextFile("file:///C:/Users/BuleSky/Desktop/data2")
    sc.stop()
  }

//  r.getString(1), r.getString(2), r.getDate(3), r.getString(4),
//          r.getString(5), r.getString(6), r.getString(7), r.getString(8),
//          r.getString(9), r.getString(10), r.getString(11), r.getString(12),
//          r.getString(13), r.getString(14), r.getString(15), r.getString(16),
//          r.getString(17), r.getString(18), r.getString(19), r.getString(20),
//          r.getString(21), r.getString(24)
  case class Bak(person_name: String,
    cetf_id: String,
    birthday: Date,
    sex: String,
    disable_card: String,
    phone: String,
    city: String,
    area: String,
    street: String,
    juwei: String,
    address: String,
    live_city: String,
    live_area: String,
    live_street: String,
    live_juwei: String,
    live_address: String,
    zip_code: String,
    live_zip: String,
    bank_name: String,
    bank_card: String,
    keeper_name: String,//21
//    sala:String,
    keeper_cetf_id: String) //22
// keeper_phone: String,  //23
//    id: String  24
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