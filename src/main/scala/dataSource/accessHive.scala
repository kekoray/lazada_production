package dataSource

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
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
    spark.sql("USE spark_test")
    //    spark.sql("select * from student").show()

    // 创建操作
    //    spark.sql("CREATE DATABASE IF NOT EXISTS spark_test")
    val createSql =
    """Create External Table If Not Exists student
      |( name String,
      |  age  Int,
      |  gpa  Decimal(5,2)
      |) Comment '学生表'
      |  Partitioned By (
      |    dt String Comment '日期分区字段{"format":"yyyy-MM-dd"}')
      |  Row Format Delimited
      |    Fields Terminated By '\t'
      |    Lines Terminated By '\n'
      |  Stored As textfile
      |  Location '/dataset/hive'
      |  Tblproperties ("orc.compress" = "SNAPPY")""".stripMargin
    //    spark.sql(createSql)


    // 写入操作
    // 1.设置schema
    val schema = StructType(
      List(StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", DoubleType)))

    // 2.读取csv文件
    val csvDF: DataFrame = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv("src/main/resources/student.csv")

    val result: Dataset[Row] = csvDF.select("name","id","age","gpa")
    //      .where("age > 18")


    /*
    按照数据位置顺序插入数据,忽略列名,前提要允许所有字段使用动态分区 [hive.exec.dynamic.partition.mode=nonstrict]
    A. overwrite + insertInto: 只覆盖分区相同的数据
    B. append + insertInto: 表末尾追加增量数据

     */

        // 设置允许所有字段使用动态分区
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        result.write
          .mode("append")
          .insertInto("spark_test.student")


    /*
    A. overwrite + saveAsTable:
        1.表存在,schema字段数相同,会按照新schema的字段位置插入对应数据
        2.表不存在,或者表存在且schema字段数不相同,则会按照新schema进行重新建表并插入数据

    B. append + saveAsTable:
        1.表存在且表已有数据,直接在表末尾追加增量数据
        2.表存在且表无数据,报错并提议使用insertInto
        3.不存在,自动建表并插入数据

    C. error + saveAsTable:
        1.只要表存在,就抛出异常
        2.不存在,自动建表并插入数据

    D. ignore + saveAsTable:
        1.只要表存在,无论有无数据,都无任何操作
        2.表不存在,自动建表并插入数据

     */

//    result.write
//      .mode("error")
//      .saveAsTable("spark_test.student2")


    spark.stop()


  }

}
