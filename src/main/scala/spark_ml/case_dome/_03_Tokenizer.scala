package spark_ml.case_dome

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml.kn
 * @FileName: _05_param
 * @description:  TODO
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2021-12-27 16:14
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _03_Tokenizer {
   def main(args: Array[String]): Unit = {
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
     val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
     import spark.implicits._
     import org.apache.spark.sql.functions._

     // 1.数据准备
     val training = spark.createDataFrame(Seq(
       (0L, "a b c d e spark", 1.0),
       (1L, "b d", 0.0),
       (2L, "spark f g h", 1.0),
       (3L, "hadoop mapreduce", 0.0),
       (4L, "b spark who", 1.0),
       (5L, "g d a y", 0.0),
       (6L, "spark fly", 1.0),
       (7L, "was mapreduce", 0.0),
       (8L, "e spark program", 1.0),
       (9L, "a e c l", 0.0),
       (10L, "spark compile", 1.0),
       (11L, "hadoop software", 0.0)
     )).toDF("id", "text", "label")

     // 2.特征工程
     //2-1.英文分词器,默认分割使用空格
     val tokenizer: Tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokentext")
     //2-2.词频统计,词频可以作为重要特征出现
     val hashingTF = new HashingTF().setInputCol("tokentext").setOutputCol("hashFeatures")

     // 3.算法操作
     val lr: LogisticRegression = new LogisticRegression()
       .setLabelCol("label")
       .setFeaturesCol("hashFeatures")
       .setPredictionCol("predictioncol")

     // 4.管道
     val pipelineModel: PipelineModel = new Pipeline().setStages(Array(tokenizer, hashingTF, lr)).fit(training)
     val showResult: DataFrame = pipelineModel.transform(training)
     showResult.show(false)

     // 5.测试预测值
     val test = spark.createDataFrame(Seq(
       (4L, "spark i j k"),
       (5L, "l m n"),
       (6L, "mapreduce spark"),
       (7L, "apache hadoop")
     )).toDF("id", "text")
     pipelineModel.transform(test).show(false)

     spark.stop()
   }

 }
