package spark_Streaming

import java.util.Properties

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode

class MySQLSink(options: Map[String, String], outputMode: OutputMode) extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    // 从option中获取需要的参数
    val userName = options.get("userName").orNull
    val password = options.get("password").orNull
    val table = options.get("table").orNull
    val jdbcUrl = options.get("jdbcUrl").orNull

    val properties = new Properties
    properties.setProperty("user", userName)
    properties.setProperty("password", password)

    data.write.mode(outputMode.toString).jdbc(jdbcUrl, table, properties)
  }
}