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

    // 3.特征降维--pca主成分分析法,利用特征值和特征向量选择具有较高特征值对应的特征向量进行降维
    /*
    
        PCA主成分分析,是一种无监督线性数据转换技术,主要作用是特征降维,即把具有相关性的高维变量转换为线性无关的低维变量,减少数据集的维数,同时保持数据集对方差贡献最大的特征;
        所以当数据集在不同维度上的方差分布不均匀的时候,PCA最有用;

        原理:
           所谓的主成分分析,不过是在高维的空间中寻找一个低维的正交坐标系;
           比如说在三维空间中寻找一个二维的直角坐标系,那么这个二维的直角坐标系就会构成一个平面,将三维空间中的各个点在这个二维平面上做投影,就得到了各个点在二维空间中的一个表示,由此数据点就从三维降成了二维.

        API用法:
             PCA算法转换的是特征向量,需要将降维的字段转为向量字段
             setK() 主成分的数量,必须小于等于降维字段个数
     */
    // setK()
    val pca = new PCA().setInputCol("features").setOutputCol("pcaFeatures").setK(2)
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
