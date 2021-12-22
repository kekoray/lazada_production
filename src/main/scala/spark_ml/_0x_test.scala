package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.Summarizer._
import org.apache.spark.sql.types.{DataTypes, StructType}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml   
 * @FileName: _0x_test 
 * @description:  选择某一列
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-22 16:28  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _0x_test {

  case class IrisFlower(sepal_length: Double, sepal_width: Double, petal_length: Double, petal_width: Double, classlabel: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("ChiSquareTestExample").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val path = "src/main/resources/ML_data/iris.data"

    // 1.rdd + 样例类 + toDF
    val dataRdd: RDD[IrisFlower] = sc.textFile(path).map(_.split(","))
      .map(filed => IrisFlower(filed(0).toDouble, filed(1).toDouble, filed(2).toDouble, filed(3).toDouble, filed(4)))
    val df = dataRdd.toDF()

    // 2.rdd + Row类 + schema + createDF
    val rowRdd: RDD[Row] = sc.textFile(path).map(_.split(","))
      .map(filed => Row(filed(0).toDouble, filed(1).toDouble, filed(2).toDouble, filed(3).toDouble, filed(4)))

    val schema = new StructType()
      .add("sepal_length", "double", true)
      .add("sepal_width", DataTypes.DoubleType, true)
      .add("petal_length", DataTypes.DoubleType, true)
      .add("petal_width", DataTypes.DoubleType, true)
      .add("classlabel", "string", true)

    val df2: DataFrame = spark.createDataFrame(rowRdd, schema)

    // 3.read + schema
    val df3: DataFrame = spark.read.format("csv").schema(schema).load(path)


    // corr相关系数
    Correlation.



    spark.stop()


  }


}
