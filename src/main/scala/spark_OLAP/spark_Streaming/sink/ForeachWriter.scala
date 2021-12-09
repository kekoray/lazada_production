package spark_OLAP.spark_Streaming.sink

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.spark_Streaming.sink
 * @FileName: ForeachWriter 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-02 14:55  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object ForeachWriter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("hdfs").master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")


    //  -----  json  ----------
    val source: DataFrame = spark.readStream
      .option("maxFilesPerTriger", 1) // 每次读取的文件数
      .schema(userSchema)
      .json("hdfs://cdh1:8020//dataset/dataset")
      .na.fill(0)


    // ====================================================================
    // ====================================================================
    // ====================================================================
    /*
      Foreach Writer倾向于一次处理一条数据,若想要流式批量的幂等写入,就要自定义Sink源

      ----- Foreach Writer  --
     */
    //    source.writeStream.foreach(
    //      new ForeachWriter[Row] {
    //        val driver = "com.mysql.jdbc.Driver"
    //        var statement: Statement = _
    //        var connection: Connection = _
    //        val url: String = "jdbc:mysql://192.168.101.88:3306/test_base"
    //        val user: String = "root"
    //        val pwd: String = "123456"
    //
    //        // 开启连接
    //        override def open(partitionId: Long, version: Long): Boolean = {
    //          Class.forName(driver)
    //          connection = DriverManager.getConnection(url, user, pwd)
    //          this.statement = connection.createStatement
    //          true
    //        }
    //
    //        // 写入处理
    //        override def process(value: Row): Unit = {
    //          // MySQL插入语句
    //          statement.executeUpdate(s"insert into test values('${value.getAs[String]("name")}','${value.getAs[Int]("age")}')")
    //        }
    //
    //        // 关闭连接
    //        override def close(errorOrNull: Throwable): Unit = connection.close()
    //      }
    //    ).start().awaitTermination()


    // ====================================================================
    // ====================================================================
    // ====================================================================
    // ----------  自定义Sink  ------------
    source.writeStream
      .format("spark_OLAP.spark_Streaming.MySQLStreamSinkProvider") // 自定义sink类的StreamSinkProvider的class位置
      .option("checkpointLocation", "hdfs://cdh1:8020//dataset/checkpoint")
      .option("userName", "root")
      .option("password", "123456")
      .option("table", "test")
      .option("jdbcUrl", "jdbc:mysql://192.168.101.88:3306/test_base")
      .start()
      .awaitTermination()


    // ----------  自定义 Sink  ------------
    // 1.创建Sink子类
    //    class MySQLSink(options: Map[String, String], outputMode: OutputMode) extends Sink {
    //      override def addBatch(batchId: Long, data: DataFrame): Unit = {
    //        // 从option中获取需要的参数
    //        val userName = options.get("userName").orNull
    //        val password = options.get("password").orNull
    //        val table = options.get("table").orNull
    //        val jdbcUrl = options.get("jdbcUrl").orNull
    //        val properties = new Properties
    //
    //        properties.setProperty("user", userName)
    //        properties.setProperty("password", password)
    //
    //        data.write.mode(outputMode.toString).jdbc(jdbcUrl, table, properties)
    //      }
    //    }

    // 2.创建Sink注册器,获取创建Sink的必备依赖
    //    class MySQLStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {
    //
    //      override def createSink(sqlContext: SQLContext,
    //                              parameters: Map[String, String],
    //                              partitionColumns: Seq[String],
    //                              outputMode: OutputMode): Sink = {
    //        new MySQLSink(parameters, outputMode)
    //      }
    //
    //      // Sink的功能名称,可在format中使用
    //      override def shortName(): String = "mysql"
    //    }


  }


}
