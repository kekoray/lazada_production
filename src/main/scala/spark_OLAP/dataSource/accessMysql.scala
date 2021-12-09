package spark_OLAP.dataSource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.dataSource
 * @FileName: accessMysql 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-18 14:52  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessMysql {
  def main(args: Array[String]): Unit = {

    //=====================================================================================
    //===========================   读取MySQL   =============================================
    //======================================================================================
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("accessMysql").getOrCreate()

    // 1.jdbc连接的配置文件
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "123456")
    properties.put("useSSL", "false")
    // 2.需要URL,table,配置文件缺一不可,其中table项可写子查询语句
    spark.read.jdbc("jdbc:mysql://192.168.100.216:3306/spark_test", "student", properties).show()
    spark.read.jdbc("jdbc:mysql://192.168.100.216:3306/spark_test", "(select * from student where age > 20 ) as tab", properties).show()



    //======================================================================================
    //===========================   写入MySQL   =============================================
    //======================================================================================

    // 1.设置schema表头,才能写对应字段写入MySQL中
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

    // 4.保存到MySQL表中
    csvDF.write.format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:mysql://192.168.100.216:3306/spark_test")
      .option("dbtable", "student")
      .option("user", "root")
      .option("password", "123456")
      // 关闭SSL认证
      .option("useSSL", "false")
      // 按照指定列进行分区,只能设置类型为数值的列
      .option("partitionColumn", "id")
      // 确定步长的参数,lowerBound-upperBound之间的数据均分给每一个分区,小于lowerBound的数据分给第一个分区,大于upperBound的数据分给最后一个分区
      .option("lowerBound", 1)
      .option("upperBound", 60)
      // 分区数量
      .option("numPartitions", 10)
      .save()


  }

}
