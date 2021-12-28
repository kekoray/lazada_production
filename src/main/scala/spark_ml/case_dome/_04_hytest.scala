package spark_ml.case_dome

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.mllib
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml.case_dome   
 * @FileName: _04_hytest 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-28 16:05  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _04_hytest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    val df = data.toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    println(chi)
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")


    val vd = mllib.linalg.Vectors.dense(1,2,3,4,5)
    val vdResult = Statistics.chiSqTest(vd)
    println(vdResult)

  }

}
