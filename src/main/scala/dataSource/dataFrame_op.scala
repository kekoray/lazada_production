package dataSource

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
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
    val people: Seq[People] = Seq(People("tom", 11), People("tony", 33))
    val personDF: DataFrame = people.toDF()


    // ---------------------  2.通过createDataFrame创建  --------------------
    // 样例类 => df
    val df1: DataFrame = spark.createDataFrame(people)

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

    // ---------------------  1.聚合操作  --------------------
    /*
      分组后聚合操作方式
          A.使用agg配合sql.functions函数进行聚合,这种方式能一次求多个聚合值,比较方便,常用这个!!
          B.使用GroupedDataset的API进行聚合,这种方式只能求一类聚合的值,不好用
     */

    val salesDF: DataFrame = Seq(("Beijing", 2016, 100), ("Beijing", 2017, 200), ("Shanghai", 2015, 50), ("Shanghai", 2016, 150), ("Guangzhou", 2017, 50))
      .toDF("city", "year", "amount")

    val groupedDF: RelationalGroupedDataset = salesDF.groupBy('city, 'year)

    // A.能一次求多个聚合值,还能指定别名  (常用)
    groupedDF
      .agg(sum("amount") as "sum_amount", avg("amount") as "avg_amount")
      .select('city, 'sum_amount, 'avg_amount)
      .show()

    // B.只能求一类的聚合值,且不能指定别名
    groupedDF
      .sum("amount")
      .show()



    // ---------------------  2.分组方式  --------------------
    /*
      多维分组聚合操作
          group by :       对查询的结果进行分组
          grouping sets :  对分组集中指定的组表达式的每个子集执行分组操作,                      [ group by A,B grouping sets((A,B),()) 等价于==>  group by A,B union 全表聚合 ]
          rollup :         对指定表达式集滚动创建分组集进行分组操作,最后再进行全表聚合,           [ rollup(A,B) 等价于==> group by A union group by A,B union 全表聚合]
          cube :           对指定表达式集的每个可能组合创建分组集进行分组操作,最后再进行全表聚合,   [ cube(A,B) 等价于==> group by A union group by B union group by A,B union 全表聚合]
     */

    // grouping sets操作
    salesDF.createOrReplaceTempView("sales")
    spark.sql(
      """select city,year,sum(amount) as sum_amount from sales
        |group by city,year grouping sets((city,year),())
        |order by city desc,year desc""".stripMargin).show()
    /*+---------+----+----------+
      |     city|year|sum_amount|
      +---------+----+----------+
      | Shanghai|2016|       150|
      | Shanghai|2015|        50|
      |Guangzhou|2017|        50|
      |  Beijing|2017|       200|
      |  Beijing|2016|       100|
      |     null|null|       550|
      +---------+----+----------+*/


    // rollup操作
    salesDF.rollup('city, 'year)
      .agg(sum("amount") as "sum_amount")
      .sort('city asc_nulls_last, 'year desc_nulls_last)
      .show()

    spark.sql(
      """select city,year,sum(amount) as sum_amount from sales
        |group by city,year with rollup
        |order by city desc,year desc""".stripMargin).show()

    /*+---------+----+----------+
      |     city|year|sum_amount|
      +---------+----+----------+
      |  Beijing|2017|       200|
      |  Beijing|2016|       100|
      |  Beijing|null|       300|
      |Guangzhou|2017|        50|
      |Guangzhou|null|        50|
      | Shanghai|2016|       150|
      | Shanghai|2015|        50|
      | Shanghai|null|       200|
      |     null|null|       550|
      +---------+----+----------+*/


    // cube操作
    salesDF.cube('city, 'year)
      .agg(sum("amount") as "sum_amount")
      .sort('city asc_nulls_last, 'year desc_nulls_last)
      .show()

    spark.sql(
      """select city,year,sum(amount) as sum_amount from sales
        |group by city,year with cube
        |order by city desc,year desc""".stripMargin).show()

    /*+---------+----+----------+
      |     city|year|sum_amount|
      +---------+----+----------+
      |  Beijing|2017|       200|
      |  Beijing|2016|       100|
      |  Beijing|null|       300|
      |Guangzhou|2017|        50|
      |Guangzhou|null|        50|
      | Shanghai|2016|       150|
      | Shanghai|2015|        50|
      | Shanghai|null|       200|
      |     null|2017|       250|
      |     null|2016|       250|
      |     null|2015|        50|
      |     null|null|       550|
      +---------+----+----------+*/


    // =======================  DataFrame-join连接操作  =======================
    /*
      join优化:
           1.Broadcast Hash Join ： 广播Join,或者叫Map端Join,适合一张较小的表(默认10M)和一张大表进行join
           2.Shuffle Hash Join :   适合一张小表和一张大表进行join，或者是两张小表之间的join
           3.Sort Merge Join ：    适合两张较大的表之间进行join
    */

    val person: DataFrame = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")
    val cities: DataFrame = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")

    // join
    person.join(cities, person.col("id") === cities.col("id"))
      .select(person.col("id"), cities.col("name") as "city")
      .show()

    // cross
    person.crossJoin(cities)
      .where(person.col("id") === cities.col("id"))
      .show()

    // inner,left,left_outer,left_anti,left_semi,right,right_outer,outer,full,full_outer
    // left_anti操作是输出'左表独有的数据',如同 [left join + where t2.col is null] 的操作
    person.join(cities, person.col("id") === cities.col("id"), joinType = "inner")
    person.join(cities, person.col("id") === cities.col("id"), joinType = "left_anti")
    person.join(cities, person.col("id") === cities.col("id"), joinType = "left").where(cities.col("id") isNull)


    // ----------------  1.广播Join操作  -------------
    /*
    将小数据集分发给每一个Executor,让较大的数据集在Map端直接获取小数据集进行Join,这种方式是不需要进行Shuffle的,所以称之为Map端Join,或者广播join
    spark会自动实现Map端Join,依赖spark.sql.autoBroadcastJoinThreshold=10M(默认)参数,当数据集小于这个参数的大小时,会自动进行Map端Join操作
   */

    // 默认开启广播Join
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt / 1024 / 1024)
    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
    /*    00 BroadcastNestedLoopJoin BuildRight, Cross
          01 :- LocalTableScan [id#152, name#153, cityId#154]
          02 +- LocalTableScan [id#164, name#165  */

    // 关闭广播Join操作
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
    /*   00 CartesianProduct
         01 :- LocalTableScan [id#152, name#153, cityId#154]
         02 +- LocalTableScan [id#164, name#165]  */

    // 使用函数强制开启广播Join
    println(person.crossJoin(broadcast(cities)).queryExecution.sparkPlan.numberedTreeString)

    // sql版本的的广播Join,这是Hive1.6之前的写法,之后的版本自动识别小表
    // spark.sql("""select /*+ MAPJOIN (rt) */ * from person cross join cities rt""")

    // sparkRDD版本的广播Join
    val personRDD: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3)))
    val citiesRDD: RDD[(Int, String)] = spark.sparkContext.parallelize(Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou")))
    val citiesBroadcast: Broadcast[collection.Map[Int, String]] = spark.sparkContext.broadcast(citiesRDD.collectAsMap())
    personRDD.mapPartitions(
      iter => {
        val value = citiesBroadcast.value
        val result = for (x <- iter // 1.iter先赋值给x
                          if value.contains(x._3) // 2.再判断value中是否有x
                          )
          yield (x._1, x._2, value(x._3)) // 3.使用列表生成式yield生成列表
        result
      }
    ).collect().foreach(println)








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
