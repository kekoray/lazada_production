package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml   
 * @FileName: _01_veter 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-03 18:36  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _01_vector {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]").setAppName("_01_vector")
    val sc = new SparkContext(conf)

    //  构建向量
    println(Vectors.dense(4.0, 0.0, 2.0, 5.0))
    println(Vectors.sparse(4, Seq((1, 1.0), (3, 99.0))))
    println(Vectors.sparse(4, Array(1, 2), Array(1.0, 9.0)))


  }

}
