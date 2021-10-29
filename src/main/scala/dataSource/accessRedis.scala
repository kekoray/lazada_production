package dataSource

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
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
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("accessRedis")
      .config("spark.redis.host", "192.168.100.189")
      .config("spark.redis.port", "6379")
      .config("spark.redis.db", "0")
      .config("spark.redis.auth", "96548e1f-0440-4e48-8ab9-7bb1e3d45238") // redis密码
      .config("redis.timeout", "2000")
      .getOrCreate()

    import spark.implicits._

    // 读取
    val sc: SparkContext = spark.sparkContext
    import com.redislabs.provider.redis._
    val setRDD: RDD[(String, String)] = sc.fromRedisKV("*") // 获取string类型数据
    setRDD.foreach(println)

//    spark.read
//      .format("org.apache.spark.sql.redis")
//      .option("table", "person")
//      .option("key.column", "name")
//      .load()
//      .show()

    // 写入
    val data: RDD[(String, String)] = sc.makeRDD(Seq(("1", "lili"), ("2", "mimi"), ("3", "popo")))
//    sc.toRedisSET(data)

//    data.toDF().write
//      .format("org.apache.spark.sql.redis")
//      .mode("Overwrite")
//      .option("key.column", "name")
//      .option("table", "person")
//      .save()


    spark.stop()


  }

}
