package dataSource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("accessRedis")
      .config("spark.redis.host", "192.168.100.189")
      .config("spark.redis.port", "6379")
      .config("spark.redis.db", "1")
      .config("spark.redis.auth", "96548e1f-0440-4e48-8ab9-7bb1e3d45238") // redis密码
      .config("redis.timeout", "2000")
      .config("spark.port.maxRetries", "1000")
      .getOrCreate()

    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    import com.redislabs.provider.redis._

    // 获取相匹配的key
    sc.fromRedisKeys(Array("abc", "customer_name"), 3).collect().foreach(println) // ??不懂

    // 获取匹配规则的key
    sc.fromRedisKeyPattern("customer*", 5).collect().foreach(println)

    // 返回kv值
    sc.fromRedisKV("customer_gender").collect().foreach(println)
    sc.fromRedisKV("customer*").collect().foreach(println)

    sc.fromRedisList("customer_phone").collect().foreach(println)
    sc.fromRedisHash("gps").collect().foreach(println)
    sc.fromRedisHash("user:1000").collect().foreach(println)
    sc.fromRedisSet("user:1000:*").collect().foreach(println)
    sc.fromRedisSet(Array("user:1000:email", "user:1000:phones")).collect().foreach(println)


    import com.redislabs.provider.redis._
    sc.toRedisKV(Seq(("1", "kkk")).toDS().rdd)
    sc.toRedisHASH(Seq(("1", "kkk")).toDS().rdd, "hsahKEY")

    val df: DataFrame = Seq(("John", 30), ("Peter", 45)).toDF("name", "age")
    df.write
      .format("org.apache.spark.sql.redis")
      .mode("overwrite")
      .option("table", "person") // 一级key名 [ person:随机数 ==> (name,John) (age,30) ]
      .option("key.column", "name") // 二级key名  [ person:John ==> (age,30) ]
      .save()

    // sql形式
    val ddlView = "CREATE TEMPORARY VIEW person2 (name STRING, age INT) USING org.apache.spark.sql.redis OPTIONS (table 'person2', key.column 'name')"
    spark.sql(ddlView)
    val queryView = "INSERT INTO TABLE person2 VALUES ('John', 63),('Peter', 65)"
    spark.sql(queryView)




    //    val redisRDD: RDD[(String, String)] = sc.fromRedisKV("*") // 获取string类型数据
    //    print(redisRDD.collect())

    //    spark.read
    //      .format("org.apache.spark.sql.redis")
    //      .option("table", "person")
    //      .option("key.column", "name")
    //      .load()
    //      .show()

    // 写入操作
    //    val data: RDD[(String, String)] = sc.makeRDD(Seq(("1", "lili"), ("2", "mimi")))
    //    sc.toRedisLIST(sc.makeRDD(Seq("lili","or","zz")), "list_name")
    //    sc.toRedisHASH(data,"hash_name")

    // 写入
    //    data.toDF("id","name").write
    //      .format("org.apache.spark.sql.redis")
    //      .option("table", "hash_name")
    //      .option("key.column", "name")
    //      .mode("Overwrite")
    //      .save()


    spark.stop()


  }

}
