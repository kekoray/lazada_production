package spark_ml.kn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml.kn   
 * @FileName: _06_decisionTree 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2022-01-07 10:37  
 * @Copyright (c) 2022,All Rights Reserved.
 */ object _06_decisionTree {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // =================  RDD方式  ================


    // =================  DataFrame方式  ============
    // 1.读取文件
    val dataDF = spark.read.format("libsvm").load("src/main/resources/ML_data/sample_libsvm_data.txt")
    // 2.查看数据信息
    dataDF.show(20, false)
    dataDF.printSchema()
    /*
      root
       |-- label: double (nullable = true)
       |-- features: vector (nullable = true)
    */

    // 3.数据集划分
    val Array(trainingSet, testSet) = dataDF.randomSplit(Array(0.8, 0.2))

    // 4.连续值数据的离散化
    val binarizer = new Binarizer().setInputCol("features").setOutputCol("features_binarizer").setThreshold(0.5)

    // 5.决策树算法
    val decisionTreeRegressor: DecisionTreeRegressor = new DecisionTreeRegressor().setFeaturesCol("features_binarizer").setLabelCol("label").setPredictionCol("predictioncol")
      .setMaxDepth(5)
      .setMaxBins(2)

    // 6.管道操作
    val pipeline: Pipeline = new Pipeline().setStages(Array(binarizer, decisionTreeRegressor))
    val pipelineModel = pipeline.fit(trainingSet)
    val trainingDF = pipelineModel.transform(trainingSet)


    // 7.模型校验器
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
      .setLabelCol("label")
      .setPredictionCol("predictioncol")

    // 超参数选择--交叉验证和网格搜索
    // -1.网格搜索的参数设置
    val paramMaps = new ParamGridBuilder()
      .addGrid(decisionTreeRegressor.maxDepth, Array(2, 5, 8, 10))
      .addGrid(decisionTreeRegressor.maxBins, Array(5, 10, 20, 30))
      .build()

    // -2.交叉验证
    val validator = new CrossValidator()
      .setNumFolds(3)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramMaps)
      .setEstimator(pipeline)
    val validatorModel: CrossValidatorModel = validator.fit(trainingSet)
    val beatParam = validatorModel.bestModel.asInstanceOf[PipelineModel].stages(1).extractParamMap()
    println("训练集对最佳超参数为  ==> " + beatParam)
    /*
    最佳超参数为  ==> {
          dtr_ecc9b4e17265-cacheNodeIds: false,
          dtr_ecc9b4e17265-checkpointInterval: 10,
          dtr_ecc9b4e17265-featuresCol: features_binarizer,
          dtr_ecc9b4e17265-impurity: variance,
          dtr_ecc9b4e17265-labelCol: label,
          dtr_ecc9b4e17265-maxBins: 5,
          dtr_ecc9b4e17265-maxDepth: 2,
          dtr_ecc9b4e17265-maxMemoryInMB: 256,
          dtr_ecc9b4e17265-minInfoGain: 0.0,
          dtr_ecc9b4e17265-minInstancesPerNode: 1,
          dtr_ecc9b4e17265-predictionCol: predictioncol,
          dtr_ecc9b4e17265-seed: 926680331
        }
     */

    // 保存模型
    println("校验模型的准确率为  ==> " + evaluator.evaluate(trainingDF))
    pipelineModel.write.overwrite().save("src/main/resources/model_dir")

    // 模型读取
    val model = PipelineModel.load("src/main/resources/model_dir")
    val testDF = model.transform(testSet)
    println(" 测试集查看模型效果  ==> ")
    testDF.select('label, 'predictioncol).show(20, false)
    println("测试集对校验模型的准确率为  ==> " + evaluator.evaluate(testDF))

    spark.stop()

  }

}
