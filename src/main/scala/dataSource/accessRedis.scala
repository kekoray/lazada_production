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

    // 整合redis的配置
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("accessRedis")
      .config("spark.redis.host", "192.168.100.189")
      .config("spark.redis.port", "6379")
      .config("spark.redis.db", "1")
      .config("spark.redis.auth", "96548e1f-0440-4e48-8ab9-7bb1e3d45238")
      .config("redis.timeout", "2000")
      .config("spark.port.maxRetries", "1000")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext


    // redisAPI的隐式转换
    import spark.implicits._
    import com.redislabs.provider.redis._


    // ---------------------  读写操作  ------------------------------
    // KV处理的是string类型数据

    // 获取相匹配的key
    sc.fromRedisKeys(Array("customer_gender")).collect().foreach(println) // 搞不懂什么用处
    sc.fromRedisKeyPattern("customer*").collect().foreach(println)

    // 根据key获取对应的value值
    sc.fromRedisKV("customer_gender").collect().foreach(println)
    sc.fromRedisKV("customer*").collect().foreach(println)
    sc.fromRedisList("customer_phone").collect().foreach(println)
    sc.fromRedisHash("gps").collect().foreach(println)
    sc.fromRedisHash("user:1000").collect().foreach(println)
    sc.fromRedisSet("user:1000:*").collect().foreach(println)
    sc.fromRedisSet(Array("user:1000:email", "user:1000:phones")).collect().foreach(println)


    // ---------------------  写入操作  ------------------------------
    sc.toRedisLIST(Seq("lili", "or", "zz").toDS().rdd, "list_name")
    sc.toRedisHASH(Seq(("1", "lili"), ("2", "mimi")).toDS().rdd, "hash_name")


    // ---------------------  hash表操作  ------------------------------
    val df: DataFrame = Seq(("John", 30), ("Peter", 45)).toDF("name", "age")

    // sql形式写入,但是不能select读取
    spark.sql(
      """CREATE TEMPORARY VIEW person2
        |(name STRING, age INT)
        |USING org.apache.spark.sql.redis
        |OPTIONS (table 'person2', key.column 'name')""".stripMargin)
    spark.sql("""INSERT INTO TABLE person2 VALUES ('John', 63),('Peter', 65)""")

    // hash表写入
    df.write
      .format("org.apache.spark.sql.redis")
      .mode("overwrite")
      .option("table", "person") // 指定一级key名 [ person:随机数 ==> (name,John) (age,30) ]
      .option("key.column", "name") // 指定二级key名  [ person:John ==> (age,30) ]
      .save()
    /*127.0.0.1:6379[1]> keys 'person*'
            1) "person:Peter"
            2) "person:John"
      127.0.0.1:6379[1]> hgetall 'person:Peter'
            1) "age"
            2) "45"                 */

    // hash表读取
    spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "person")
      .option("key.column", "name")
      .load()
      .show()
    /*+-----+---+
      | name|age|
      +-----+---+
      |Peter| 45|
      | John| 30|
      +-----+---+*/



    spark.stop()


  }

}
