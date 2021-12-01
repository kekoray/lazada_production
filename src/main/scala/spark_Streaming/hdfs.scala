package spark_Streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_Streaming   
 * @FileName: StructuredStreaming 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-01 16:09  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object hdfs {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("hdfs").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    val userSchema = new StructType()
      .add("name", "string")
      .add("age", "integer")


    val source: DataFrame = spark.readStream
      .schema(userSchema)
      .json("hdfs://cdh1:8020//dataset/dataset")

    source.distinct()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()


  }

}
