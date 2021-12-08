
package spark_Streaming.source

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BooleanType, DateType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SQLContext, SparkSession}
import spark_Streaming.MySQLSink

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_Streaming.source   
 * @FileName: agg_notebook 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-06 17:46  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object agg_notebook {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("hdfs").master("local[8]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // =========================================================
    // =======================  Source  =======================
    // *********************************************************

    // 定义schema表信息
    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")


    // =======================  json  =======================
    val jsonSource = spark.readStream
      .option("maxFilesPerTriger", 10) // 每次读取的文件数
      .schema(userSchema)
      .json("hdfs://cdh1:8020//dataset/dataset")


    // =======================  csv  =======================
    val csvSource = spark.readStream
      .option("sep", ",")
      .schema(userSchema)
      .csv("hdfs://cdh1:8020//dataset/dataset")


    /*  =======================  kafka  ======================================
        读取Kafka消息的三种方式(startingOffsets):
                1.earliest: 从每个Kafka分区最开始处开始获取
                2.assign: 手动指定每个Kafka分区中的Offset
                3.latest: 不再处理之前的消息,只获取流计算启动后新产生的数据
     */
    val kafkaSource: DataFrame = spark.readStream
      .format("kafka") // 设置为Kafka指定使用KafkaSource读取数据
      .option("kafka.bootstrap.servers", "192.168.101.88:9092,192.168.101.88:9093,192.168.101.88:9094") // 指定Kafka的Server地址
      .option("subscribe", "test") // 要监听的Topic,匹配模式("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest") // 读取Kafka消息的方式
      .load()

    /*
        Kafka消息的解析处理过程:
                1.从获取Kafka的Schema信息可知,其中Kafka消息的Value是业务数据处理的主体内容,其为json格式;
                2.利用多个StructType子嵌套构建json数据的Schema,再通过from_json函数反序列化解析Value中的json数据;
                3.再通过点(.)形式获取所需的值,进行数据的业务处理;

        Kafka的Schema信息:
                root
                  |-- key: binary (nullable = true)                      Kafka消息的Key
                  |-- value: binary (nullable = true)                    Kafka消息的Value
                  |-- topic: string (nullable = true)                   本条消息所在的Topic,因为整合的时候一个Dataset可以对接多个Topic,所以有这样一个信息
                  |-- partition: integer (nullable = true)              消息的分区号
                  |-- offset: long (nullable = true)                    消息在其分区的偏移量
                  |-- timestamp: timestamp (nullable = true)            消息进入Koafka的时间戳
                  |-- timestampType: integer (nullable = true)          时间戳类型

        Kafka中的JSON数据:
                   {"devices": {
                           "cameras": {
                               "device_id": "awJo6rH",
                               "last_event": {
                                   "has_sound": true,
                                   "has_motion": true,
                                   "has_person": true,
                                   "start_time": "2016-12-29T00:00:00.000Z",
                                   "end_time": "2016-12-29T18:42:00.000Z"
                                   }
                                }
                            }
                     }
    */

    kafkaSource.printSchema()

    val eventType = new StructType()
      .add("has_sound", BooleanType, nullable = true)
      .add("has_motion", BooleanType, nullable = true)
      .add("has_person", BooleanType, nullable = true)
      .add("start_time", DateType, nullable = true)
      .add("end_time", DateType, nullable = true)
    val camerasType = new StructType()
      .add("device_id", StringType, nullable = true)
      .add("last_event", eventType, nullable = true)
    val devicesType = new StructType()
      .add("cameras", camerasType, nullable = true)

    // 利用多个StructType子嵌套构建json数据的Schema
    val schema = new StructType()
      .add("devices", devicesType, nullable = true)

    // 可选项options: json数据中包含Date类型的数据,所以要指定时间格式化方式
    val jsonOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.sss'Z'")

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // Kafka消息的解析处理
    val kafkaResult = kafkaSource.selectExpr("CAST(value AS STRING) as value") // 1.获取kafka消息的value
      .select(from_json('value, schema, jsonOptions).alias("parsed_value")) // 2.通过from_json函数反序列化解析Value中的json数据
      .selectExpr("parsed_value.devices.cameras.last_event.has_person as has_person", // 3.通过点(.)形式直接获取所需值
        "parsed_value.devices.cameras.last_event.start_time as start_time")
      .filter('has_person === true) // 4.数据的业务处理
      .groupBy('has_person, 'start_time)
      .count()





    // ========================================================
    // =======================  Sink  =======================
    // ********************************************************


    /*  =======================  Tigger触发器  =======================
        Tigger触发器:
           A.流查询触发器可以设置流数据处理的时间,分为具有'固定批处理间隔的微批处理查询'和'连续处理查询';
           B.从Spark2.3版本开始支持连续流处理,连续流处理可以做到大约1ms的端到端数据处理延迟,且可以做到at-least-once的容错语义,但是Source和Sink只能限制在Kafka用;
                1.默认不指定Tigger,则是按顺序批次处理;
                2.Trigger.ProcessingTime("1 seconds"): 按照固定时间间隔划分批次处理;
                3.Trigger.Once():  一次性批次处理,只执行一个微批处理来处理所有可用数据,然后自行停止;
                4.Trigger.Continuous("1 second"): 连续流处理,查询将在新的低延迟,连续处理模式下执行;
    */
//    jsonSource.writeStream
//      .format("console")
//      .outputMode("complete")
//      .trigger(Trigger.ProcessingTime("1 seconds"))     // 1.按照固定时间间隔划分批次处理
//      .trigger(Trigger.Once())                          // 2.一次性批次处理
//      .trigger(Trigger.Continuous("1 second"))          // 3.连续流处理
//      .start()
//      .awaitTermination()


    // =======================  console  ===========================
    jsonSource.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
      .awaitTermination()


    /*  =======================  hdfs  ============================
        HDFS文件系统中复制或移动出现瞬态文件的报错问题: [File does not exist: xxxx.json._COPYING_]
            1.xxxx.json._COPYING_是在复制过程正在进行时创建的临时瞬态文件,等待复制过程完成,就会消失;
            2.最好的解决方案是将文件移动到HDFS中的临时位置,然后将它们移动到目标目录;
    */
    jsonSource.writeStream
      .format("csv") // 选项可为"orc","json","csv"等
      .option("path", "hdfs://cdh1:8020//dataset/sinkout")
      .option("checkpointLocation", "hdfs://cdh1:8020//dataset/checkpoint")
      .start()
      .awaitTermination()


    // =======================  kafka  ===========================
    kafkaSource.writeStream
      .format("kafka")
      .outputMode("Append")
      .option("kafka.bootstrap.servers", "192.168.101.88:9092,192.168.101.88:9093,192.168.101.88:9094")
      .option("topic", "streaming-test")
      .start()
      .awaitTermination()


    /*  =======================  foreach-writer  =================================
        在Structured Streaming中,并未提供完整的MySQL/JDBC/第三方存储系统的整合工具,所以需要自己编写sink
        foreach-writer倾向于一次处理一条数据,若想要流式批量的幂等写入,就要重写类实现自定义Sink
    */

    // 匿名内部类的方式操作
    jsonSource.writeStream.foreach(
      new ForeachWriter[Row] {
        val driver = "com.mysql.jdbc.Driver"
        var statement: Statement = _
        var connection: Connection = _
        val url: String = "jdbc:mysql://192.168.101.88:3306/test_base"
        val user: String = "root"
        val pwd: String = "123456"

        // 开启连接
        override def open(partitionId: Long, version: Long): Boolean = {
          Class.forName(driver)
          connection = DriverManager.getConnection(url, user, pwd)
          this.statement = connection.createStatement
          true
        }

        // 写入处理
        override def process(value: Row): Unit = {
          statement.executeUpdate(s"insert into test values('${value.getAs[String]("name")}','${value.getAs[Int]("age")}')")
        }

        // 关闭连接
        override def close(errorOrNull: Throwable): Unit = connection.close()
      }
    ).start().awaitTermination()


    // =======================  自定义sink  =======================
    // ---------------------  1.创建Sink子类  --------------------
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

    // ---------------------  2.创建Sink注册器,获取创建Sink的必备依赖  --------------------
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

    // ---------------------  3.调用自定义sink  --------------------
    jsonSource.writeStream
      .format("spark_Streaming.MySQLStreamSinkProvider") // 自定义sink的DataSourceRegister的class位置
      .option("checkpointLocation", "hdfs://cdh1:8020//dataset/checkpoint")
      .option("userName", "root")
      .option("password", "123456")
      .option("table", "test")
      .option("jdbcUrl", "jdbc:mysql://192.168.101.88:3306/test_base")
      .start()
      .awaitTermination()
  }

}
