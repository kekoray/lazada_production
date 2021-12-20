package spark_ml

import org.apache.calcite.rel.core.Correlate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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


    // =============  统计特征  =================
    // 统计特征: 均值,方差,非0值的个数等..
    val data = sc.textFile("src/main/resources/ML_data/testdata.txt")
    val vector: RDD[linalg.Vector] = data.map(_.split(" ").map(_.toDouble)).map(Vectors.dense(_))

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

    // 2.构建相关系数矩阵 (工作常用)
    val matrix: Matrix = Statistics.corr(vector, method = "pearson")
    println("==>" + matrix)
    /*
        1.0  1.0  1.0
        1.0  1.0  1.0
        1.0  1.0  1.0   */


    import spark.implicits._
    val data3 = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    val featuresDF: DataFrame = data3.map(Tuple1.apply).toDF("features")



    // ===============  随机数  ===============


  }

}
