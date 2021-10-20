import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}

import scala.util.parsing.json.JSON

/*
 *
 * @ProjectName: lazada_production
 * @program:
 * @FileName: WordCounts
 * @description: TODO
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2021-09-07 14:45
 * @Copyright (c) 2021,All Rights Reserved.
 */

object WordCounts {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")

    val spark: SparkSession = new SparkSession.Builder()
      // hive配置
//      .config("spark.sql.warehouse.dir", "hdfs://cdh1:8020/user/hive/warehouse")
//      .config("hive.metastore.uris", "thrift://cdh1:9083")
      .appName("wordCount")
      // 支持hive操作
//      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    //    val source: RDD[String] = sc.textFile("src/main/resources/wordcount.txt")
    //    val words: RDD[String] = source.flatMap(_.split(" "))
    //    val wordsTuple: RDD[(String, Int)] = words.map((_, 1))
    //    val result: RDD[(String, Int)] = wordsTuple.reduceByKey(_ + _)
    //
    //    val df: DataFrame = result.map(x => Words(x._1, x._2)).toDF("word", "num")
    //    df.show()

    //    df.write
    //      .mode(SaveMode.Overwrite)
    //      .partitionBy("num")
    //      //      .bucketBy(12,"num")
    //      //      .sortBy("num")
    //      //        .saveAsTable("test")
    //      .format("csv")
    //      .save("src/main/resources/input")

    //    val csvDF: DataFrame = spark.read.csv("src/main/resources/input")
    //    csvDF.show()
    //    csvDF.repartition(1)
    //        .write
    //        .json("src/main/resources/input_json")
    //    val peopleDataset: Dataset[String] = spark.createDataset("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    //    spark.read.json(peopleDataset).show()


    //  读取jsonl文件
    //    val dataFrame: DataFrame = spark.read.json("src/main/resources/item.jsonl")

    //    //    =====================  JSON文件读取处理  =========================
    //
    // 1.json ==> rdd[T] : 利用JSON-API解析成Map类型数据,再封装到样例类中
    val jsonRdd: RDD[String] = sc.textFile("src/main/resources/item.jsonl")
    // 使用Scala中有自带JSON库解析,返回对象为Some(map: Map[String, Any])
    val jsonSomeRdd: RDD[Option[Any]] = jsonRdd.map(JSON.parseFull(_))
    // 将数据转换为Map类型
    val jsonMap: RDD[Map[String, Any]] = jsonSomeRdd.map(
      r => r match {
        case Some(map: Map[String, Any]) => map
        case _ => null
      })
    // 将数据封装到样例类中
    val PayRdd: RDD[Pay] = jsonMap.map(x => Pay(x("amount").toString, x("memberType").toString, x("orderNo").toString, x("payDate").toString, x("productType").toString))
    val dataSet: Dataset[Pay] = PayRdd.toDS()











    // =====================  column对象  =========================

    // ---------------  column创建方式  -------------------
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 创建Column对象
    dataSet
      .select('name) // 常用
      .select($"name")
      .select(col("name"))
      .select(column("name"))
      .where('age > 0)
      .where("age > 0")

    // 创建关联此Dataset的Column对象
    dataSet.col("addCol")
    dataSet.apply("addCol2")
    dataSet("addCol2")


    // ---------------  column常用操作  -----------------
    // 1.类型转换
    dataSet.select('age.as[String])

    // 2.创建别名
    dataSet.select('name.as("other_name"))

    // 3.添加列
    dataSet.withColumn("double_age", 'age * 2)

    // 4.模糊查找
    dataSet.select('name.like("apple"))

    // 5.是否存在指定列
    dataSet.select('name.isin("a", "b"))

    // 6.正反排序
    dataSet.sort('age.asc)
    dataSet.sort('age.desc)





    // ---------------  创建  -------------------


    //    // 2.json ==> DataFrame  :  利用sparkSQL的json方法
    //    spark.read.json("src/main/resources/item.jsonl")


    //    spark.sql("show databases").show()


    //    mysql读取
    //    val df: DataFrame = spark.read.format("jdbc")
    //      .option("url", "jdbc:mysql://192.168.100.216:3306/mysql")
    //      // dbtable可写表名,也可写子查询语句
    //      .option("dbtable", "(select * from innodb_table_stats where sum_of_other_index_sizes > 0) as tab")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      // 关闭SSL认证
    //      .option("useSSL", "false")
    //      // 按照指定列进行分区,只能设置类型为数值的列
    //      .option("partitionColumn", "age")
    //      // 确定步长的参数,lowerBound-upperBound之间的数据均分给每一个分区,小于lowerBound的数据分给第一个分区,大于upperBound的数据分给最后一个分区
    //      .option("lowerBound", 1)
    //      .option("upperBound", 60)
    //      // 分区数量
    //      .option("numPartitions", 10)
    //      .load()
    //    df.show()


    //    Thread.sleep(100000)
    sc.stop()

  }

  case class Words(word: String, num: Int)

  case class Pay(amount: String, memberType: String, orderNo: String, payDate: String, productType: String)

}
