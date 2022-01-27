package spark_ml.case_dome

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml.case_dome   
 * @FileName: _05_demo 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2022-01-18 16:09  
 * @Copyright (c) 2022,All Rights Reserved.
 */ object _05_demo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_02_IrisHeaderSparkSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    spark.read.format("libsvm").load("")


  }

}
