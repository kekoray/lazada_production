package spark_OLAP.dataSource

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.dataSource
 * @FileName: accessFile 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-18 14:52  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessFile {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("accessFile").master("local[*]").getOrCreate()


    //==========================================================================================
    //===========================   读取JSON文件   =============================================
    //==========================================================================================

    // ------------  1.json ==> rdd[T]:利用JSON-API解析成Map类型数据,再封装到样例类中  ---------------
    val sc = spark.sparkContext
    // 1.rdd读取文件
    val jsonRDD: RDD[String] = sc.textFile("src/main/resources/item.jsonl")
    // 2.使用Scala中有自带JSON库解析,返回对象为Some(map: Map[String, Any])
    val jsonParseRDD: RDD[Option[Any]] = jsonRDD.map(JSON.parseFull(_)) // Some(Map(payDate -> 2020-11-30 19:50:42, ... ))
    // 3.将Some数据转换为Map类型
    val jsonMapRDD: RDD[Map[String, Any]] = jsonParseRDD.map(
      r => r match {
        case Some(map: Map[String, Any]) => map
        case _ => null
      }
    )
    // 4.将数据封装到样例类中
    val PayRdd: RDD[Pay] = jsonMapRDD.map(x => Pay(x("amount").toString, x("memberType").toString, x("orderNo").toString, x("payDate").toString, x("productType").toString))
    PayRdd.foreach(println(_)) // Pay(40000.0,105,7E84FF304B45455894999A3FD9449093,2020-11-30 19:50:42,88.0)


    // ------------  2.json ==> DataFrame:利用sparkSQL的json方法  ---------------
    val jsonDF: DataFrame = spark.read.json("src/main/resources/item.jsonl")
    jsonDF.show()


    //==========================================================================================
    //===========================   写入JSON文件   =============================================
    //==========================================================================================

    // 当底层有多个文件时,repartition重新分区只输出1个文件
    jsonDF.repartition(1)
      .write.json("src/main/resources/output/json")


  }

  case class Pay(amount: String, memberType: String, orderNo: String, payDate: String, productType: String)

}
