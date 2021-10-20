import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program:    
 * @FileName: tset_1 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-20 9:26  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object tset_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://cdh1:8020/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://cdh1:9083")
//      .config("spark.sql.parquet.writeLegacyFormat", "true")
//      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
//      .config("hive.exec.dynamici.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql
    val data = Array(("001", "张三", 21, "2011"), ("003", "kk", 33, "2017"))
    val df = spark.createDataFrame(data).toDF("id", "name", "age", "year")
    //创建临时表
    df.createOrReplaceTempView("temp_table")
    val tableName = "test_partition"
    //切换hive的数据库
    sql("use spark_test")
    //    1、创建分区表，并写入数据
//        df.write.mode("overwrite").partitionBy("year").saveAsTable(tableName)
    df.write.mode("append").insertInto(tableName)

    spark.table(tableName).show()
    val data1 = Array(("011", "Sam", 21, "2018"),("011", "Sam1", 21, "2019"),("011", "Sam2", 21, "2019"))
    val df1 = spark.createDataFrame(data1).toDF("id", "name", "age", "year")
    df1.write.mode("overwrite")
      .insertInto(tableName)
    spark.table(tableName).show()


  }

}
