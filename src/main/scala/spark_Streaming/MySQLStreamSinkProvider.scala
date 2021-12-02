package spark_Streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class MySQLStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {

      override def createSink(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              partitionColumns: Seq[String],
                              outputMode: OutputMode): Sink = {
        new MySQLSink(parameters, outputMode)
      }

      // Sink的功能名称,可在format中使用
      override def shortName(): String = "mysql"
    }