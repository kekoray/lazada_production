package spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
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
    val wordCount: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(x => (x, 1))

    //    wordCount.print()


    // ===================  updateStateByKey操作  =================
    /*
      updateStateByKey函数可以进行带历史状态的计算,但是需要chekcpoint保存当前计算的结果;
     */

    // 1.使用updateStateByKey必须设置Checkpoint目录
    ssc.checkpoint("src/main/resources/streaming")

    // 2.编写updateStateByKey的函数体
    def updateFunc(newValue: Seq[Int], runningValue: Option[Int]) = {
      // newValue是当前批次Key的所有Value的序列集合
      val currentBatchSum: Int = newValue.sum
      // runningValue是key对应的以前的值
      val state: Int = runningValue.getOrElse(0)
      // 返回key的最新值,会再次进入Checkpoint中当作状态存储
      Some(currentBatchSum + state)
    }

    // 3.初始化状态值,可以将上一次程序结束的结果状态保存到MySQL中,等下次执行时读取状态值并继续累加即可,这算是updateStateByKey的优化
    val sc: SparkContext = SparkContext.getOrCreate(config)
    val initialRDD: RDD[(String, Int)] = sc.parallelize(Array(("hadoop", 100), ("spark", 25)))

    // 4.调用updateStateByKey
    val wordCounts: DStream[(String, Int)] = wordCount.updateStateByKey(updateFunc(_, _), new HashPartitioner(ssc.sparkContext.defaultParallelism), initialRDD)
    wordCounts.print()



    // ===================  window操作  =================
    /*
        windowDuration窗口长度:窗口长度配置的就是将多长时间内的RDD合并为一个
        slideDuration滑动间隔:每隔多久生成一个window

        滑动时间的问题:
            1.如果windowDuration > slideDuration,则在每一个不同的窗口中,可能计算了重复的数据
            2.如果windowDuration < slideDuration,则在每一个不同的窗口之间,有一些数据未能计算进去
     */

    // 通过window操作,会将流分为多个窗口
    //    val wordsWindow: DStream[(String, Int)] = wordCount.window(Seconds(30), Seconds(10))

    // 此时是针对于窗口求聚合
    //    val wordCounts = wordsWindow.reduceByKey((newValue, runningValue) => newValue + runningValue)

    //    /* 如上操作等同于 ==>  */
    //    // 开启窗口并进行reduceByKey聚合,需要写对应的参数名 (reduceFunc,windowDuration窗口长度,slideDuration滑动间隔)
    //    val wordCounts: DStream[(String, Int)] = wordCount.reduceByKeyAndWindow(reduceFunc = (_ + _), windowDuration = Seconds(5), slideDuration = Seconds(5))
    //    wordCounts.print()


    // 开始接收数据并处理数据
    ssc.start()
    // 等待处理被终止
    ssc.awaitTermination()
    // 手动停止处理
    ssc.stop()


  }

}
