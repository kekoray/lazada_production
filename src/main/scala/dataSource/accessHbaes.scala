package dataSource

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program: dataSource   
 * @FileName: accessHbaes 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-18 15:12  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessHbaes {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("accessHbaes").master("local[*]").getOrCreate()
      val sc: SparkContext = spark.sparkContext

      HBaseConfiguration.create()

    }
  }
}
