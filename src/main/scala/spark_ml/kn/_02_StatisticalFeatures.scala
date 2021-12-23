package spark_ml.kn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.stat.Summarizer.{max, mean, normL1, variance}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.random.RandomRDDs.normalRDD
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{ml, mllib}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml
 * @FileName: _02_StatisticalFeatures
 * @description:  TODO
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2021-12-09 15:57
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _02_StatisticalFeatures {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_01_vector").master("local[6]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")


    // =============  统计特性  =================
    // 统计特征: 均值,方差,非0值的个数等..

    // 1.rdd方式--统计特性
    val data = sc.textFile("src/main/resources/ML_data/testdata.txt")
    val vector: RDD[linalg.Vector] = data.map(_.split(" ").map(_.toDouble)).map(mllib.linalg.Vectors.dense(_))

    // 通过colStats按列汇总统计信息,然后才能使用均值,方差等的统计方法;
    val summary: MultivariateStatisticalSummary = Statistics.colStats(vector)
    println(summary.max)
    println(summary.min)
    println(summary.count)
    println(summary.mean) // 均值
    println(summary.numNonzeros) // 非0个数
    println(summary.variance) // 方差
    println(summary.normL1) // L1范数(曼哈段距离):每列的各个元素绝对值之和
    println(summary.normL2) // L2范数(欧几里得距离):每列的各元素的平方和然后求平方根


    /* 2.DataFrame方式--统计特性
       DataFrame方式的ml.stat.Summarizer统计特征,要在spark-mllib/spark-sql在2.4.0版本以上才有的新功能
      导包路径:  import org.apache.spark.ml.stat.Summarizer._
    */
    import spark.implicits._

    val dataDF: DataFrame = Seq(
      (ml.linalg.Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (ml.linalg.Vectors.dense(4.0, 6.0, 7.0), 2.0)
    ).toDF("features", "weight")

    val (meanVal, varianceVal, maxVal, normL1Val) = dataDF.select(mean('features), variance('features), max('features), normL1('features))
      .as[(Vector, Vector, Vector, Vector)].first()

    println(s" mean = ${meanVal}, variance = ${varianceVal}, max = ${maxVal}, normL1 = ${normL1Val}")


    // ===============  统计相关系数  ===============
    /*   统计相关系数
             cos余弦相似度
             Pearson皮尔逊相关系数是在cos余弦相似度的基础上做了标准化的操作

         作用:构建相关系数矩阵,可以进行降维
            假如特征A和特征B的相似度为1.0,即完全相似,那么使用A或者B对模型决策是没有太大的影响,故使用特征A或者特征B其中一个特征就可以表示2个特征了;
            假如y=k1x1+k2x2,若x2=2x1,故y=k1x1+k2(2x1)=x1(k1+2k2)
   */
    // 1.统计两个RDD的相关系数,两个输入的RDD需要有相同数量的分区,并且每个分区中的元素数量相同
    val data1: RDD[Double] = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
    val data2: RDD[Double] = sc.parallelize(Seq(2.0, 4.0, 6.0, 8.0))
    val corr: Double = Statistics.corr(data1, data2)
    println("==>" + corr) // 1.0000000000000002


    // 2.rdd方式--构建相关系数矩阵 (工作常用)
    val matrix: Matrix = Statistics.corr(vector, method = "pearson")
    println("==>" + matrix)
    /*
        1.0  1.0  1.0
        1.0  1.0  1.0
        1.0  1.0  1.0   */


    // 3.DataFrame方式--构建相关系数矩阵
    // 与mllib包的RDD类型代码公用的时候,注意两种类型的Vectors导包问题,可能会报错
    import spark.implicits._
    val data3: Seq[ml.linalg.Vector] = Seq(
      ml.linalg.Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      ml.linalg.Vectors.dense(4.0, 5.0, 0.0, 3.0),
      ml.linalg.Vectors.dense(6.0, 7.0, 0.0, 8.0),
      ml.linalg.Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    val featuresDF: DataFrame = data3.map(Tuple1.apply).toDF("features")
    val corrDF: DataFrame = Correlation.corr(featuresDF, "features")
    corrDF.show(false)
    /*
        +------------------------------------------------------------------------
        |pearson(features)
        +------------------------------------------------------------------------
        1.0                     0.055641488407465814   NaN    0.4004714203168137
        0.055641488407465814    1.0                    NaN    0.9135958615342522
        NaN                     NaN                    1.0    NaN
        0.4004714203168137      0.9135958615342522     NaN    1.0
        +------------------------------------------------------------------------
     */


    // ===============  随机数  ===============
    /*
       seed随机数种子,保证随机选择数据的可重复性,即无论程序重跑多少次,随机结果始终不会变
       随机数作用:
             1.随机数可以用于随机生成制定分布方式的数据
             2.随机数可以用于将数据集进行训练集和测试集的拆分
     */
    // 1.生成标准正态分布的随机数
    val normalData: RDD[Double] = normalRDD(sc, 10L, 1, 456L)
    normalData.foreach(println(_))

    // 2.拆分数据集
    val arrayData: Array[RDD[Double]] = normalData.randomSplit(Array(0.8, 0.2), 123L)
    val trainingSet: RDD[Double] = arrayData(0)
    val testSet: RDD[Double] = arrayData(1)


  }

}
