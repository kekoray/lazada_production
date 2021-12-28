package spark_ml.kn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{ChiSqSelector, MinMaxScaler, PCA, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml.kn   
 * @FileName: _04_pipeline 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-14-27 11:24  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _04_pipeline {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    /*
       todo: 总结
         1.fit或者transformer的用法:    (一般情况下先尝试fit,没有再transform即可)
             a.如果一个特征操作继承自Estimator,就需要实现fit+tranform方法
             b.如果一个特征操作继承自Transform,就只要实现tranform方法


      管道的五大属性:
        1.DataFrame,只有在DataFrame数据集上才能实现管道的操作;
        2.Estimate,需要先fit形成mode,再通过transform进行转换;
        3.transformer,只需要通过transform直接转换;
        4.pipeline,将数据的各个处理流程串联起来操作;
        5.Parameter超参数,模型训练之前指定的参数,如迭代次数等;


      参数:模型训练的数值
      超参数:模型训练之前指定的参数,如迭代次数等;
          设置方式:
              1.若对1个算法实例,可直接set方法指定参数,如lr.setMaxlter(10);
              2.若有多个算法实例,可用ParamMap方式指定多个参数,如ParamMap(lr1.maxlter -> 10,lr2.maxlter -> 10);

    */

    // 1.数据读取
    val path = "src/main/resources/ML_data/iris.csv"
    val irisData: DataFrame = spark.read.option("header", true).csv(path)


    // 2.数据类型的转换
    val irisDF = irisData.select(
      'sepal_length.cast("Double"),
      'sepal_width.cast("Double"),
      'petal_length.cast("Double"),
      'petal_width.cast("Double"),
      'class
    )

    // 3.训练集和测试集的划分
    val dataArray = irisDF.randomSplit(Array(0.8, 0.2))
    val trainingSet = dataArray(0)
    val testSet = dataArray(1)

    // 4.特征工程
    // 4-1.类别数据的数值化
    val stringIndexer = new StringIndexer().setInputCol("class").setOutputCol("classlabel")
    // 4-2.特征组合
    val vectorAssembler = new VectorAssembler().setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width")).setOutputCol("features")
    // 4-3.特征降维
    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(2)
    // 4-4.数值归一化
    val minMaxScaler = new MinMaxScaler().setInputCol("pcaFeatures").setOutputCol("minmaxFeatures")
    // 4-5.卡方选择
    val chiSqSelector = new ChiSqSelector().setFeaturesCol("minmaxFeatures").setLabelCol("classlabel").setOutputCol("ChiSq").setNumTopFeatures(1)

    // 5.管道操作
    val pipelineModel: PipelineModel = new Pipeline().setStages(Array(stringIndexer, vectorAssembler, pca, minMaxScaler, chiSqSelector)).fit(trainingSet)
    val pipelineResultDF = pipelineModel.transform(trainingSet)
    pipelineResultDF.show(false)

    // 6.测试集的预测值输出
    pipelineModel.transform(testSet).show(false)

    spark.stop()
  }

}
