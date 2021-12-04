//package spark_Streaming.source
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
///*
// *
// * @ProjectName: lazada_production
// * @program: spark_Streaming
// * @FileName: StructuredStreaming
// * @description:  TODO
// * @version: 1.0
// * *
// * @author: koray
// * @create: 2021-12-01 16:09
// * @Copyright (c) 2021,All Rights Reserved.
// */ object hdfs {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    val spark = SparkSession.builder().appName("hdfs").master("local[8]").getOrCreate()
//    spark.sparkContext.setLogLevel("ERROR")
//
//
//    val userSchema = new StructType()
//      .add("name", "string")
//      .add("age", "integer")
//
//
//    //  -----  json  ----------
//    val source: DataFrame = spark.readStream
//      .option("maxFilesPerTriger", 10) // 每次读取的文件数
//      .schema(userSchema)
//      .json("hdfs://cdh1:8020//dataset/dataset")
//
//
//    //    ----  csv  --------
//    spark.readStream
//      .option("sep", ",")
//      .schema(userSchema)
//      .csv("hdfs://cdh1:8020//dataset/dataset")
//
//
//
//
//    /*
//      HDFS文件系统中复制或移动出现瞬态文件的报错问题:
//          报错: [File does not exist: xxxx.json._COPYING_]
//          xxxx.json._COPYING_是在复制过程正在进行时创建的临时瞬态文件,等待复制过程完成,就会消失;
//          最好的解决方案是将文件移动到HDFS中的临时位置,然后将它们移动到目标目录;
//     */
//
//
//    // -------  hdfs-sink  -------------
//    //    source.writeStream
//    //      .format("csv") // 选项可为"orc","json","csv"等
//    //      .option("path", "hdfs://cdh1:8020//dataset/sinkout")
//    //      .option("checkpointLocation", "hdfs://cdh1:8020//dataset/checkpoint")
//    //      .start()
//    //      .awaitTermination()
//
//
//    // ===================================================================
//    // ===================================================================
//    // ===================================================================
//    /*
//        Tigger触发器:
//         流查询触发器可以设置流数据处理的时间,分为具有 固定批处理间隔的微批处理查询 和 连续处理查询;
//              1.默认不指定Tigger,则是按顺序批次处理;
//              2.Trigger.ProcessingTime("1 seconds"): 按照固定时间间隔划分批次处理;
//              3.Trigger.Once():  一次性批次处理,只执行一个微批处理来处理所有可用数据,然后自行停止;
//              4.Trigger.Continuous("1 second"): 连续流处理,查询将在新的低延迟,连续处理模式下执行;
//         从Spark2.3版本开始支持连续流处理,连续流处理可以做到大约1ms的端到端数据处理延迟,且可以做到at-least-once的容错语义,
//         但是Source和Sink只能限制再Kafka用;
//
//     */
//
//    // -------  输出控制台  -------------
//    source.distinct()
//      .writeStream
//      .format("console")
//      .outputMode("complete")
//      .trigger(Trigger.ProcessingTime("1 seconds")) // 按照固定时间间隔划分批次处理
//      .trigger(Trigger.Once()) // 一次性批次处理
//      .trigger(Trigger.Continuous("1 second")) // 连续流处理
//      .start()
//      .awaitTermination()
//
//
//  }
//
//}
