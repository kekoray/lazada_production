package dataSource

import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program: dataSource   
 * @FileName: accessRedis 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-18 15:12  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessRedis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("accessRedis").getOrCreate()




  }

}
