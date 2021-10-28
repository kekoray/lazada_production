package dataSource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset, Row, SaveMode, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: dataSource   
 * @FileName: dataFrame_op 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-28 16:21  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object dataFrame_op {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local[*]").appName("dataFrame_op").getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._


    // *****************************************************************************************************
    // *****************************************  spark-sql  ***********************************************
    // *****************************************************************************************************


    // =======================================================================
    // ===========================  DataFrame  ===============================
    // =======================================================================


    // =======================  DataFrame-创建方式  =======================

    // ---------------------  1.通过隐式转换创建(toDF)  --------------------
    // rdd => df
    val dataRdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(Seq(("tom", 11), ("tony", 33)))
    val rddDF: DataFrame = dataRdd.toDF()

    // Seq => df
    val SeqDF: DataFrame = Seq(("tom", 11), ("tony", 33)).toDF("name", "age")

    // 样例类 => df  (常用)
    val person: Seq[People] = Seq(People("tom", 11), People("tony", 33))
    val personDF: DataFrame = person.toDF()


    // ---------------------  2.通过createDataFrame创建  --------------------
    // 样例类 => df
    val df1: DataFrame = spark.createDataFrame(person)

    // rowRDD+schema => df
    val rdd: RDD[Row] = dataRdd.map(x => Row(x._1, x._2))
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))
    val df2: DataFrame = spark.createDataFrame(rdd, schema)


    // ---------------------  3.通过读取外部文件创建(read)  --------------------
    // read => df
    val df3: DataFrame = spark.read
      .format("csv")
      .option("header", true)
      .load("./文件路径")



    // =======================  DataFrame处理操作  =======================
    // A.命令式API操作
    df1.select().show()

    // B.SQL操作 -- 注册一张临时表,然后用SQL操作这张临时表
    df1.createOrReplaceTempView("tmp_table")
    spark.sql("""select * from tmp_table""").show()



    // =======================  DataFrame-读写数据操作  =======================

    // ---------------------  1.读取外部数据源  --------------------
    // A.format+load
    val df4: DataFrame = spark.read
      .format("csv")
      .option("header", true)
      .load("./文件路径")

    // B.封装方法(csv,jdbc,json)
    val df5: DataFrame = spark.read
      .option("header", true)
      .csv("./文件路径")


    // ---------------------  2.写入外部数据源  --------------------
    /*
      读写模式(mode):
          SaveMode.Overwrite:       覆盖写入
          SaveMode.Append:          末尾写入
          SaveMode.Ignore:          若已经存在, 则不写入
          SaveMode.ErrorIfExists:   已经存在, 则报错
     */

    // A.format+save
    df1.repartition(1) // 当底层多文件时,重新分区只输出1个文件
      .write
      .mode("error")
      .format("json")
      .save("./文件路径")

    // B.封装方法(csv,jdbc,json)
    df2.write
      .mode(saveMode = "error") // 写入模式
      .option("encoding", "UTF-8") // 外部参数
      .partitionBy(colNames = "year", "month") // 文件夹分区操作,分区字段被指定后,再也不出现在数据中
      .json(path = "./文件路径")

    // C.saveAsTable
    df2.write
      .mode(SaveMode.Overwrite)
      .partitionBy(colNames = "year", "month") // 文件夹分区操作
      .bucketBy(numBuckets = 12, colName = "month") // 文件分桶+排序的组合操作,只能在saveAsTable中使用
      .sortBy(colName = "month")
      .saveAsTable("表名")


    // =======================  DataFrame-缺失值处理  =======================
    /*
      常见的缺失值有两种:
          1. Double.NaN类型的NaN和字符串对象null等特殊类型的值,也是空对象,一般使用drop,fill处理
          2. "Null", "NA", " "等为字符串类型的值,一般使用replace处理
     */

    val df: DataFrame = List((1, "kk", 18.0), (2, "", 25.0), (3, "UNKNOWN", Double.NaN), (4, "null", 30.0))
      .toDF("id", "name", "age")


    // ---------------------  1.删除操作  --------------------
    /*
       how选项:
          any ==> 处理的是当某行数据任意一个字段为空
          all ==> 处理的是当某行数据所有值为空
     */

    //1.默认为any,当某一列有NaN时就删除该行
    df.na.drop().show()
    df.na.drop("any").show()
    df.na.drop("any", List("id", "name")).show()

    //2.all,所有的列都是NaN时就删除该行
    df.na.drop("all").show()


    // ---------------------  2.填充操作  --------------------
    // 对包含null和NaN的所有列数据进行默认值填充,可以指定列;
    df.na.fill(0).show()
    df.na.fill(0, List("id", "name")).show()


    // ---------------------  3.替换操作  --------------------
    // 将包含'字符串类型的缺省值'的所有列数据替换成其它值,被替换的值和新值必须是同类型,可以指定列;
    //1.将“名称”列中所有出现的"UNKNOWN"替换为"unnamed"
    df.na.replace("name", Map("UNKNOWN" -> "unnamed")).show()

    //2.在所有字符串列中将所有出现的"UNKNOWN"替换为"unnamed"
    df.na.replace("*", Map("UNKNOWN" -> "unnamed")).show()


    // =======================  DataFrame-多维分组聚合操作  =======================

    // ---------------------  1.分组操作方式  --------------------
    /*
      多维分组聚合操作
          group by :        对查询的结果进行分组
          grouping sets :   对分组集中指定的组表达式的每个子集执行分组操作,                      [ group by A,B grouping sets((A,B),()) 等价于==>  group by A,B union 全表聚合 ]
          rollup :          对指定表达式集滚动创建分组集进行分组操作,最后再进行全表聚合,           [ rollup(A,B) 等价于==> group by A union group by A,B union 全表聚合]
          cube :            对指定表达式集的每个可能组合创建分组集进行分组操作,最后再进行全表聚合,   [ cube(A,B) 等价于==> group by A union group by B union group by A,B union 全表聚合]


     */


    // ---------------------  2.分组后聚合操作方式  --------------------
    /*
      分组后聚合操作方式
        1.使用agg配合sql.functions函数进行聚合,这种方式能一次求多个聚合值,比较方便,常用这个!!
        2.使用GroupedDataset的API进行聚合,这种方式只能求一类聚合的值,不好用
     */

    val salesDF: DataFrame = Seq(
      ("Beijing", 2016, 100),
      ("Beijing", 2017, 200),
      ("Shanghai", 2015, 50),
      ("Shanghai", 2016, 150),
      ("Guangzhou", 2017, 50)
    ).toDF("city", "year", "amount")

    val groupedDF: RelationalGroupedDataset = salesDF.groupBy('city, 'year)

    // 常用,能一次求多个聚合值,还能指定别名
    groupedDF
      .agg(sum("amount") as "sum_amount", avg("amount") as "avg_amount")
      .select('city, 'sum_amount, 'avg_amount)
      .show()

    // 只能求一类的聚合值,且不能指定别名
    groupedDF
      .sum("amount")
      .show()




    // =======================================================================
    // ===========================  DataSet  ===============================
    // =======================================================================


    // ---------------------  DataSet创建方式  --------------------


    // =======================================================================
    // ===========================  DS,DF,RDD转换  ============================
    // =======================================================================

    // 1.RDD[T]转换为Dataset[T]/DataFrame
    val peopleRDD: RDD[People] = spark.sparkContext.makeRDD(Seq(People("zhangsan", 22), People("lisi", 15)))
    val peopleDS: Dataset[People] = peopleRDD.toDS()
    val peopleDF: DataFrame = peopleDS.toDF()

    // 2.Dataset[T]/DataFrame互相转换
    val peopleDF_fromDS: DataFrame = peopleDS.toDF()
    val peopleDS_fromDF: Dataset[People] = peopleDF.as[People]

    // 4.Dataset[T]/DataFrame转换为RDD[T]
    val RDD_fromDF: RDD[Row] = peopleDF.rdd
    val RDD_fromDS: RDD[People] = peopleDS.rdd


  }

  case class People(name: String, age: Int)

}
