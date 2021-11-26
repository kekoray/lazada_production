//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
///*
// *
// * @ProjectName: lazada_production
// * @program:
// * @FileName: nanop
// * @description:  TODO
// * @version: 1.0
// * *
// * @author: koray
// * @create: 2021-10-28 18:03
// * @Copyright (c) 2021,All Rights Reserved.
// */ object nanop {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    val spark: SparkSession = SparkSession.builder().appName("dataFrame_op").master("local[*]").getOrCreate()
//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//
//    val df: DataFrame = List((1, "kk", 18.0), (2, "", 25.0), (3, "UNKNOWN", Double.NaN), (4, "NA", 0.0))
//      .toDF("id", "name", "age")
//
////    df.show()
//
//    df.select('id, 'name, 'age)
//      .where('name =!= "NA")
//      .show
//  }
//
//}
