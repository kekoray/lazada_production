package dataSource

import java.time.LocalDate

import org.apache.spark.sql.functions.expr
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


    // spark整合hive的连接配置
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("accessHive")
      .config("spark.sql.warehouse.dir", "hdfs://cdh1:8020/user/hive/warehouse") // 设置WareHouse的位置
      .config("hive.metastore.uris", "thrift://cdh1:9083") // 设置MetaStore的位置
      .enableHiveSupport() // 开启Hive支持
      .config("hive.exec.dynamic.partition.mode", "nonstrict") // 设置动态分区模式
      .getOrCreate()

    // 隐私转换
    import org.apache.spark.sql.functions._
    import spark.implicits._


    //======================================================================================
    //===========================   创建操作   =============================================
    //======================================================================================
    spark.sql("USE spark_test")
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
    spark.sql(createSql)


    //======================================================================================
    //===========================   写入操作   =============================================
    //======================================================================================

    val data = Array(("张三", 21, 2.1), ("李四", 16, 1.2), ("王五", 18, 5.3))
    val dataDF: DataFrame = spark.createDataset(data).toDF("name", "age", "gpa")
    val result: Dataset[Row] = dataDF.select("name", "age", "gpa").where("age > 20")

    // 设置dt分区字段
    val resultDF: DataFrame = result.withColumn("dt", lit(LocalDate.now().toString.substring(0, 7)))
    resultDF.show()

    /* ------------------------  1.insertInto模式  ------------------------------
      insertInto模式是按照数据位置顺序插入数据,前提是要设置动态分区模式 [config("hive.exec.dynamic.partition.mode", "nonstrict")]
      若操作的是分区表,不用指定partitionBy(),会自动获取分区字段,从而插入对应分区的数据;
      注意!! :  如果操作过[overwrite+saveAsTable]后,则会无法找到分区字段,overwrite+insertInto就会覆盖全表;
        A. overwrite + insertInto : 在不影响其他分区数据的情况下,只覆盖指定分区的数据;
        B. append + insertInto : 在表末尾追加增量数据
    */

    // 常用操作,相当于(Inset Overwrite .. Partition ..)
    resultDF.write
      .mode("overwrite")
      .insertInto("spark_test.student")


    /* ------------------------  2.saveAsTable模式  ------------------------------
    A. overwrite + saveAsTable:
        1.表存在,schema字段数相同,会按照新schema字段位置,覆盖全表插入数据
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

    resultDF.write
      .mode("append")
      .partitionBy("dt")
      .saveAsTable("spark_test.student")


    //======================================================================================
    //===========================   查询操作   =============================================
    //======================================================================================
    spark.sql("select * from student").show()
    spark.table("student").show()

    import spark.sql
    sql("select * from student").show()



    spark.stop()

  }

}
