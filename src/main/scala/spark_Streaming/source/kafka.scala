package spark_Streaming.source

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{BooleanType, DateType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_Streaming
 * @FileName: kafka
 * @description:  TODO
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2021-12-01 17:39
 * @Copyright (c) 2021,All Rights Reserved.
 */ object kafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("kafka").master("local[6]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    val kafkasource: DataFrame = spark.readStream
      .format("kafka") // 设置为Kafka指定使用KafkaSource读取数据
      .option("kafka.bootstrap.servers", "192.168.101.88:9092,192.168.101.88:9093,192.168.101.88:9094") // 指定Kafka的Server地址
      .option("subscribe", "test") // 要监听的Topic,匹配模式("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest") // 从什么位置开始获取数据
      .load()


    kafkasource.printSchema()
    /*
        Kafka获取到的DataFrame的Schema信息:
            root
             |-- key: binary (nullable = true)                      Kafka消息的Key
             |-- value: binary (nullable = true)                    Kafka消息的Value
             |-- topic: string (nullable = true)                   本条消息所在的Topic,因为整合的时候一个Dataset可以对接多个Topic,所以有这样一个信息
             |-- partition: integer (nullable = true)              消息的分区号
             |-- offset: long (nullable = true)                    消息在其分区的偏移量
             |-- timestamp: timestamp (nullable = true)            消息进入Koafka的时间戳
             |-- timestampType: integer (nullable = true)          时间戳类型
    */

    // =======================  反序列化json数据: 利用多个StructType嵌套成json数据的schema  ========================
    /*
    kafka中的JSON数据:
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

    // 多个schema嵌套成json数据
    val schema = new StructType()
      .add("devices", devicesType, nullable = true)


    // JSON中包含Date类型的数据,所以要指定时间格式化方式
    val jsonOptions = Map("timestampFormat" -> "yyyy-MM-dd'T'HH:mm:ss.sss'Z'")

    import org.apache.spark.sql.functions._

    val result = kafkasource.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
      .select(from_json('value, schema, jsonOptions).alias("parsed_value")) // 使用from_json函数格式化JSON,可以直接获取其中某些字段的值
      .selectExpr("parsed_value.devices.cameras.last_event.has_person as has_person", "parsed_value.devices.cameras.last_event.start_time as start_time")
      .filter('has_person === true)
      .groupBy('has_person, 'start_time)
      .count()

    //    result.writeStream
    //      .outputMode(OutputMode.Complete())
    //      .format("console")
    //      .start()
    //      .awaitTermination()




    result.writeStream
      .format("kafka")
      .outputMode("Append")
      .option("kafka.bootstrap.servers", "192.168.101.88:9092,192.168.101.88:9093,192.168.101.88:9094")
      .option("topic", "streaming-test")
      .start()
      .awaitTermination()


  }

}
