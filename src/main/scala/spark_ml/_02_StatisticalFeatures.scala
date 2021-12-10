package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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


    //
    val data1: RDD[Double] = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0))
    val data2: RDD[Double] = sc.parallelize(Seq(2.0, 4.0, 6.0, 8.0))
    val corr: Double = Statistics.corr(data1, data2)
    println("==>" + corr)

    // 构建相关系数矩阵
    val matrix: Matrix = Statistics.corr(vector, method = "pearson")
    println("==>" + matrix)


  }

}
