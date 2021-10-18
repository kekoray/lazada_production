package dataSource

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: dataSource   
 * @FileName: accessHive 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-18 14:52  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessHive {
  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("accessHive")
      // 设置WareHouse的位置
      .config("spark.sql.warehouse.dir", "hdfs://cdh1:8020/user/hive/warehouse")
      // 设置MetaStore的位置
      .config("hive.metastore.uris", "thrift://cdh1:9083")
      // 开启Hive支持
      .enableHiveSupport()
      .getOrCreate()

    // 查询操作
//    spark.sql("show databases").show()

    // 创建操作
//    spark.sql("CREATE DATABASE IF NOT EXISTS spark_test")
    spark.sql("USE spark_test")
    val createSql ="""Create External Table If Not Exists student
               |( name String,
               |  age  Int,
               |  gpa  Decimal(5,2)
               |) Comment '学生表'
               |  Partitioned By (
               |    `dt` String Comment '日期分区字段{"format":"yyyy-MM-dd"}')
               |  Row Format Delimited
               |    Fields Terminated By '\t'
               |    Lines Terminated By '\n'
               |  Stored As textfile
               |  Location '/dataset/hive'
               |  Tblproperties ("orc.compress" = "SNAPPY")""".stripMargin
    spark.sql(createSql)

//
//    // 写入操作
//    // 1.设置schema
//    val schema = StructType(
//      List(StructField("id", IntegerType),
//        StructField("name", StringType),
//        StructField("age", IntegerType),
//        StructField("gpa", DoubleType)))
//
//    // 2.读取csv文件
//    val csvDF: DataFrame = spark.read
//      .option("delimiter", "\t")
//      .schema(schema)
//      .csv("src/main/resources/student.csv")
//
//    csvDF.select("name", "age", "gpa").show()

//    csvDF.write.mode("overwrite")
//      .saveAsTable("student")

    spark.stop()


  }

}
