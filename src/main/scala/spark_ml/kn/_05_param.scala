package spark_ml.kn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionSummary}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
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
 * @create: 2021-12-27 17:12  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _05_param {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    /*
       网格搜索和交叉验证: 主要是解决超参数的选择问题

      交叉验证:
          1.简单交叉验证: 主要用于训练模型,将数据集随机拆分为训练集和验证集,并使用验证集上的评估指标来选择最佳模型;
          2.K则交叉验证: 主要用于超参数选择,将数据集平均随机切分为k等分,拿其中一份数据作为训练集,一份数据作为验证集,从而得到最佳模型;
          3.留一验证
    */

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
    // 2-1.英文分词器,默认分割使用空格
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("text").setOutputCol("tokentext")
    // 2-2.词频统计,词频可以作为重要特征出现
    val hashingTF = new HashingTF().setInputCol("tokentext").setOutputCol("hashFeatures")

    // 3.算法操作
    val lr: LogisticRegression = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("hashFeatures")
      .setPredictionCol("predictioncol")


    // 4.管道操作
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
    val pipelineModel: PipelineModel = pipeline.fit(training)


    // 5.交叉验证和网格搜索
    // 5-1.设置网格搜索需要搜索的参数,一般都是在默认值的左右两侧选定
    val paramMaps: Array[ParamMap] = new ParamGridBuilder()
      .addGrid(lr.maxIter, Array(101, 99, 110))
      .addGrid(lr.regParam, Array(0.01, 0.001))
      .build()

    // 5-2.模型校验器,因为分类问题的准确率就是标准,所以设置指标名称为标准验证accuracy;
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("predictioncol")
      .setMetricName("accuracy")

    // 5-3.交叉验证
    // ---------------  5-3-1.k折交叉验证  --------------------------
    val crossValidator: CrossValidator = new CrossValidator()
      .setNumFolds(3) // 数据集划分的份数
      .setEstimator(pipeline) // 需要搜索的Estimator
      .setEstimatorParamMaps(paramMaps) // 需要搜索的参数
      .setEvaluator(evaluator) // 模型校验器,做交叉验证是为了验证哪个参数好
    val crossValidatorModel = crossValidator.fit(training)


    // ---------------  5-3-2.训练集交叉验证  --------------------------
    val trainValidationSplit: TrainValidationSplit = new TrainValidationSplit()
      .setTrainRatio(0.8) // 训练集和验证集的占比
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramMaps)
      .setEvaluator(evaluator)
    val trainValidationSplitModel = trainValidationSplit.fit(training)


    // 5-4.获取算法的最佳超参数信息
    /*
        从(K则交叉验证模型)/(训练集交叉验证模型)中选出最佳模型,并将其转换为PipelineModel类型,获取阶段3的lr算法中的最佳参数信息;
         bestModel: 从k则交叉验证中选择出最佳的模型
         extractParamMap(): 获取的参数的映射情况
     */
    println(crossValidatorModel.bestModel.asInstanceOf[PipelineModel].stages(2).extractParamMap())
    println(trainValidationSplitModel.bestModel.asInstanceOf[PipelineModel].stages(2).extractParamMap())
    /*
        {
          logreg_21dce36e16a4-aggregationDepth: 2,                    聚合深度
          logreg_21dce36e16a4-elasticNetParam: 0.0,                   弹性净参数
          logreg_21dce36e16a4-family: auto,
          logreg_21dce36e16a4-featuresCol: hashFeatures,             特征列
          logreg_21dce36e16a4-fitIntercept: true,
          logreg_21dce36e16a4-labelCol: label,                     标签列
          logreg_21dce36e16a4-maxIter: 101,                        迭代次数
          logreg_21dce36e16a4-predictionCol: predictioncol,        预测列
          logreg_21dce36e16a4-probabilityCol: probability,         概率列
          logreg_21dce36e16a4-rawPredictionCol: rawPrediction,     原始预测列
          logreg_21dce36e16a4-regParam: 0.01,
          logreg_21dce36e16a4-standardization: true,               标准化
          logreg_21dce36e16a4-threshold: 0.5,
          logreg_21dce36e16a4-tol: 1.0E-6
        }
     */


    // 6.超参数的设置应用
    // ---------------  6-1.set方式直接设置参数  --------------------------
    lr.setMaxIter(101).setRegParam(0.01)

    // ---------------  6-2.fit方式设置参数  --------------------------
    // 6-2-1.指定多个参数的Map
    val paramMap = ParamMap(lr.maxIter -> 101, lr.regParam -> 0.01)
      .put(lr.threshold -> 0.55)
    // 6-2-2.fit方式设置参数,会覆盖set方式
    pipeline.fit(training, paramMap).transform(training).show(false)

    // 7.模型保存
    pipelineModel.write.overwrite().save("src/main/resources/model_dir/")

    // 8.模型加载
    PipelineModel.load("src/main/resources/model_dir/").transform(training)

    spark.stop()
  }

}
