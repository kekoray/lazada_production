package spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_Streaming   
 * @FileName: StreamingWordCount 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-11-24 15:27  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // 传参有问题报错
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }

    // 流入口
    val config = new SparkConf().setAppName("StreamingWordCount").setMaster("local[6]")
    val ssc: StreamingContext = new StreamingContext(config, Seconds(1))

    // 设置socket的信息
    val lines = ssc.socketTextStream(hostname = args(0), port = args(1).toInt, storageLevel = StorageLevel.MEMORY_ONLY)

    // wordcount操作
    val wordCount = lines.flatMap(_.split(" "))
      .map(x => (x, 1))

    //    wordCount.print()

    // 使用 updateStateByKey 必须设置 Checkpoint 目录
    ssc.checkpoint("src/main/resources/streaming")


    def updateFunc(newValue: Seq[Int], runningValue: Option[Int]) = {
      val currentBatchSum: Int = newValue.sum
      val state: Int = runningValue.getOrElse(0)
      Some(currentBatchSum + state)
    }

    // updateStateByKey
    val wordCounts: DStream[(String, Int)] = wordCount.updateStateByKey[Int](updateFunc)
    wordCounts.print()




    // 开始接收数据并处理数据
    ssc.start()
    // 等待处理被终止
    ssc.awaitTermination()
    // 手动停止处理
    ssc.stop()


  }

}
