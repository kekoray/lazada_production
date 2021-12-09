package spark_OLAP.spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.spark_Streaming
 * @FileName: StructuredStreaming 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-01 16:09  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("StructuredStreaming").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    val socketlist: Dataset[String] = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", "1234")
      .load()
      .as[String]

    val words: Dataset[(String, Long)] = socketlist.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey(_._1)
      .count()

    words.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()


  }

}
