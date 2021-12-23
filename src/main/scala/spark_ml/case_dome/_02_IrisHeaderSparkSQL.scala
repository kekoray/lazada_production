package spark_ml.case_dome

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{ChiSqSelector, MinMaxScaler, PCA, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml.case_dome   
 * @FileName: _02_IrisHeaderSparkSQL 
 * @description:  带有header的schema如何直接读取schema
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-23 11:04  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _02_IrisHeaderSparkSQL {

  /*
   todo: 总结
     1.fit或者transformer的用法:    (一般情况下先尝试fit,没有再transform即可)
         a.如果一个特征操作继承自Estimator,就需要实现fit+tranform方法
         b.如果一个特征操作继承自Transform,就只要实现tranform方法


  */

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val path = "src/main/resources/ML_data/iris.csv"
    val irisData: DataFrame = spark.read.option("header", true).csv(path)
    //    irisData.printSchema()
    // 数据类型转换
    val irisDF = irisData.select(
      'sepal_length.cast("Double"),
      'sepal_width.cast("Double"),
      'petal_length.cast("Double"),
      'petal_width.cast("Double"),
      'class
    )
    //    irisDF.show(false)


    // 特征工程
    // 1.类别数据的数值化
    val stringIndexer = new StringIndexer().setInputCol("class").setOutputCol("classlabel")
    val stringIndexDF = stringIndexer.fit(irisDF).transform(irisDF)

    // 2.特征组合
    val vectorAssembler = new VectorAssembler().setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width")).setOutputCol("features")
    val featureeDF = vectorAssembler.transform(stringIndexDF)
    //    featureeDF.show(false)

    // 3.特征降维--pca主成分分析法，利用特征值和特征向量选择具有较高特征值对应的特征向量进行降维
    // setK() 主成分的数量
    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(4)
    val pcaFeaturesDF = pca.fit(featureeDF).transform(featureeDF)
    //    pcaFeaturesDF.show(false)

    // 4.数值归一化--MinMacScaler
    val minMaxScaler = new MinMaxScaler().setInputCol("pcaFeatures").setOutputCol("minmaxFeatures")
    val scalerDF = minMaxScaler.fit(pcaFeaturesDF).transform(pcaFeaturesDF)
    //    scalerDF.show(false)

    // 5.卡方选择
    val chiSqSelector = new ChiSqSelector().setFeaturesCol("minmaxFeatures").setLabelCol("classlabel").setOutputCol("ChiSq").setNumTopFeatures(2)
    val chisqDF = chiSqSelector.fit(scalerDF).transform(scalerDF)
    //    chisqDF.show(50, false)

    // ========================  管道操作  ==================================
    val pipeline: Pipeline = new Pipeline().setStages(Array(stringIndexer, vectorAssembler, pca, minMaxScaler, chiSqSelector))
    val pipelineResultDF = pipeline.fit(irisDF).transform(irisDF)
    pipelineResultDF.show(false)

    spark.stop()
  }

}
