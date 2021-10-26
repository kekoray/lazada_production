package nk_part

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: nk_part   
 * @FileName: dataFarm_op 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-26 11:27  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object dataFrame_op {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder().appName("dataFrame_op").master("local[*]").getOrCreate()
    import spark.implicits._

    // 缺失值处理
    /*
    对于缺失值的处理一般就是丢弃和填充
DataFrameNaFunctions.drop 可以在当某行中包含 null 或 NaN 的时候丢弃此行
DataFrameNaFunctions.fill 可以在将 null 和 NaN 充为其它值
DataFrameNaFunctions.replace 可以把 null 或 NaN 替换为其它值, 但是和 fill 略有一些不同, 这个方法针对值来进行替换


     */

    val df: DataFrame = List((1, "kk", 18.0), (2, "", 25.0), (3, "li", Double.NaN), (4, "null", 30.0)).toDF("id", "name", "age")

    // 丢弃
    df.na.drop().show()
//    df.na.fill().show()
//    df.na.replace().show()


  }

}
