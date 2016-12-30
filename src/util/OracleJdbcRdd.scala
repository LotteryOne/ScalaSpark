package util

import org.apache.spark.Partition
import java.sql.ResultSet
import java.sql.Connection
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import java.sql.PreparedStatement
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.TaskContext
import org.apache.spark.util

class OracleJdbcPartition(idx: Int, parameters: Map[String, Object]) extends Partition {
  override def index = idx
  val partitionParameters = parameters
}

abstract class OracleJdbcRdd[T:ClassTag] (
  sc:SparkContext,
  getConnection:() => Connection,
  sql:String,
  getOracleJdbcPatition :()=>Array[Partition],
  preparedStatement :(PreparedStatement,OracleJdbcPartition)=> PreparedStatement,
  mapRow :(ResultSet)=> T=OracleJdbcRdd.resultSetToObjectArray _)
  extends RDD[T](sc,Nil)with Logging{
  
  def getPartitions: Array[Partition] = {
    getOracleJdbcPatition();
  }
  
 
  

}

object OracleJdbcRdd {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
  
   trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }
}

