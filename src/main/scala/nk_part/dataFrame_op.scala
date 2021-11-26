//package nk_part
//
//import org.apache.hadoop.hive.ql.exec.UDF
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
//import org.junit.Test
//
///*
// *
// * @ProjectName: lazada_production
// * @program: nk_part
// * @FileName: dataFarm_op
// * @description:  TODO
// * @version: 1.0
// * *
// * @author: koray
// * @create: 2021-10-26 11:27
// * @Copyright (c) 2021,All Rights Reserved.
// */ object dataFrame_op {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    val spark: SparkSession = SparkSession.builder().appName("dataFrame_op").master("local[*]").getOrCreate()
//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//
//
//
//    // =======================  缺失值处理  =======================
//    /*
//     常见的缺失值有两种
//         1. Double.NaN类型的NaN和字符串对象null等特殊类型的值,也是空对象,一般使用drop,fill处理
//         2. "Null", "NA", " "等为字符串类型的值,一般使用replace处理
//     */
//
//    val df: DataFrame = List((1, "kk", 18.0), (2, "", 25.0), (3, "UNKNOWN", Double.NaN), (4, "null", 30.0)).toDF("id", "name", "age")
//
//    // 删除操作,删除包含null和NaN的行,可以指定列;
//    // [ any ==> 处理的是当某行数据任意一个字段为空, all ==> 处理的是当某行数据所有值为空 ]
//    //1.默认为any,当某一列有NaN时就删除该行
//    df.na.drop().show()
//    df.na.drop("any").show()
//    df.na.drop("any", List("id", "name")).show()
//
//    //2.all,所有的列都是NaN时就删除该行
//    df.na.drop("all").show()
//
//
//    // 填充操作,对包含null和NaN的所有列数据进行默认值填充,可以指定列;
//    df.na.fill(0).show()
//    df.na.fill(0, List("id", "name")).show()
//
//    // 替换操作,替换包含'字符串类型的缺省值'的所有列数据为其它值,被替换的值和新值必须是同类型,可以指定列;
//    //1.将“名称”列中所有出现的“UNKNOWN”替换为“unnamed”
//    df.na.replace("name", Map("UNKNOWN" -> "unnamed")).show()
//
//    //2.在所有字符串列中将所有出现的“ UNKNOWN”替换为“ unnamed"
//    df.na.replace("*", Map("UNKNOWN" -> "unnamed")).show()
//
//
//    //  ========================  多维分组聚合操作  =======================================
//    /*
//    1.单维聚合 group by
//    2.多维聚合 roll,cube
//
//    分组后聚合操作方式
//      1.使用agg配合sql.functions函数进行聚合,这种方式能一次求多个聚合值,比较方便,常用这个!!
//      2.使用GroupedDataset的API进行聚合,这种方式只能求一类聚合的值,不好用
//     */
//
//    import spark.implicits._
//    import org.apache.spark.sql.functions._
//
//    val salesDF: DataFrame = Seq(
//      ("Beijing", 2016, 100),
//      ("Beijing", 2017, 200),
//      ("Shanghai", 2015, 50),
//      ("Shanghai", 2016, 150),
//      ("Guangzhou", 2017, 50)
//    ).toDF("city", "year", "amount")
//
//    val groupedDF: RelationalGroupedDataset = salesDF.groupBy('city, 'year)
//
//    // 常用,能一次求多个聚合值,还能指定别名
//    groupedDF
//      .agg(sum("amount") as "sum_amount", avg("amount") as "avg_amount")
//      .select('city, 'sum_amount, 'avg_amount)
//      .show()
//
//    // 只能求一类的聚合值,且不能指定别名
//    groupedDF
//      .sum("amount")
//      .show()
//
//
//    // ---------------  分组  ---------------------
//    /*
//     多维分组聚合操作
//         group by :        对查询的结果进行分组
//         grouping sets :   对分组集中指定的组表达式的每个子集执行分组操作,                         [ group by A,B grouping sets((A,B),()) 等价于==>  group by A,B union 全表聚合 ]
//         rollup :          对指定表达式集滚动创建分组集进行分组操作,最后再进行全表聚合,             [ rollup(A,B) 等价于==> group by A union group by A,B union 全表聚合]
//         cube :            对指定表达式集的每个可能组合创建分组集进行分组操作,最后再进行全表聚合,    [ cube(A,B) 等价于==> group by A union group by B union group by A,B union 全表聚合]
//     */
//
//    // grouping sets操作
//    salesDF.createOrReplaceTempView("sales")
//    spark.sql(
//      """select city,year,sum(amount) as sum_amount from sales
//        |group by city,year grouping sets((city,year),())
//        |order by city desc,year desc""".stripMargin).show()
//    /*+---------+----+----------+
//      |     city|year|sum_amount|
//      +---------+----+----------+
//      | Shanghai|2016|       150|
//      | Shanghai|2015|        50|
//      |Guangzhou|2017|        50|
//      |  Beijing|2017|       200|
//      |  Beijing|2016|       100|
//      |     null|null|       550|
//      +---------+----+----------+*/
//
//    // rollup操作
//    salesDF.rollup('city, 'year)
//      .agg(sum("amount") as "sum_amount")
//      .sort('city asc_nulls_last, 'year desc_nulls_last)
//      .show()
//
//    spark.sql(
//      """select city,year,sum(amount) as sum_amount from sales
//        |group by city,year with rollup
//        |order by city desc,year desc""".stripMargin).show()
//
//    /*+---------+----+----------+
//      |     city|year|sum_amount|
//      +---------+----+----------+
//      |  Beijing|2017|       200|
//      |  Beijing|2016|       100|
//      |  Beijing|null|       300|
//      |Guangzhou|2017|        50|
//      |Guangzhou|null|        50|
//      | Shanghai|2016|       150|
//      | Shanghai|2015|        50|
//      | Shanghai|null|       200|
//      |     null|null|       550|
//      +---------+----+----------+*/
//
//    // cube操作
//    salesDF.cube('city, 'year)
//      .agg(sum("amount") as "sum_amount")
//      .sort('city asc_nulls_last, 'year desc_nulls_last)
//      .show()
//
//    spark.sql(
//      """select city,year,sum(amount) as sum_amount from sales
//        |group by city,year with cube
//        |order by city desc,year desc""".stripMargin).show()
//
//    /*+---------+----+----------+
//      |     city|year|sum_amount|
//      +---------+----+----------+
//      |  Beijing|2017|       200|
//      |  Beijing|2016|       100|
//      |  Beijing|null|       300|
//      |Guangzhou|2017|        50|
//      |Guangzhou|null|        50|
//      | Shanghai|2016|       150|
//      | Shanghai|2015|        50|
//      | Shanghai|null|       200|
//      |     null|2017|       250|
//      |     null|2016|       250|
//      |     null|2015|        50|
//      |     null|null|       550|
//      +---------+----+----------+*/
//
//
//    //  ========================  join连接操作  =======================================
//    /*
//    join优化:
//         1.Broadcast Hash Join ：广播Join,或者叫Map端Join,适合一张较小的表(默认10M)和一张大表进行join
//         2.Shuffle Hash Join : 适合一张小表和一张大表进行join，或者是两张小表之间的join
//         3.Sort Merge Join ： 适合两张较大的表之间进行join
//    */
//
//    val person: DataFrame = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
//      .toDF("id", "name", "cityId")
//    val cities: DataFrame = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
//      .toDF("id", "name")
//
//    // join
//    person.join(cities, person.col("id") === cities.col("id"))
//      .select(person.col("id"), cities.col("name") as "city")
//      .show()
//
//    // cross
//    person.crossJoin(cities)
//      .where(person.col("id") === cities.col("id"))
//      .show()
//
//    // inner,left,left_outer,left_anti,left_semi,right,right_outer,outer,full,full_outer
//    // left_anti操作是输出'左表独有的数据',如同 [left join + where t2.col is null] 的操作
//    person.join(cities, person.col("id") === cities.col("id"), joinType = "inner")
//    person.join(cities, person.col("id") === cities.col("id"), joinType = "left_anti")
//    person.join(cities, person.col("id") === cities.col("id"), joinType = "left").where(cities.col("id") isNull)
//
//
//    // ----------------  1.广播Join操作  -------------
//    /*
//    将小数据集分发给每一个Executor,让较大的数据集在Map端直接获取小数据集进行Join,这种方式是不需要进行Shuffle的,所以称之为Map端Join,或者广播join
//    spark会自动实现Map端Join,依赖spark.sql.autoBroadcastJoinThreshold=10M(默认)参数,当数据集小于这个参数的大小时,会自动进行Map端Join操作
//   */
//
//    // 默认开启广播Join
//    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt / 1024 / 1024)
//    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
//    /*    00 BroadcastNestedLoopJoin BuildRight, Cross
//          01 :- LocalTableScan [id#152, name#153, cityId#154]
//          02 +- LocalTableScan [id#164, name#165  */
//
//    // 关闭广播Join操作
//    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
//    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
//    /*   00 CartesianProduct
//         01 :- LocalTableScan [id#152, name#153, cityId#154]
//         02 +- LocalTableScan [id#164, name#165]  */
//
//    // 使用函数强制开启广播Join
//    println(person.crossJoin(broadcast(cities)).queryExecution.sparkPlan.numberedTreeString)
//
//    // sql版本的的广播Join,这是Hive1.6之前的写法,之后的版本自动识别小表
//    // spark.sql("""select /*+ MAPJOIN (rt) */ * from person cross join cities rt""")
//
//    // sparkRDD版本的广播Join
//    val personRDD: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 3)))
//    val citiesRDD: RDD[(Int, String)] = spark.sparkContext.parallelize(Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou")))
//    val citiesBroadcast: Broadcast[collection.Map[Int, String]] = spark.sparkContext.broadcast(citiesRDD.collectAsMap())
//    personRDD.mapPartitions(
//      iter => {
//        val value = citiesBroadcast.value
//        val result = for (x <- iter // 1.iter先赋值给x
//                          if value.contains(x._3) // 2.再判断value中是否有x
//                          )
//          yield (x._1, x._2, value(x._3)) // 3.使用列表生成式yield生成列表
//        result
//      }
//    ).collect().foreach(println)
//
//
//
//  }
//
//  @Test
//  def test(): Unit = {
//    val spark = SparkSession.builder()
//      .appName("window")
//      .master("local[6]")
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val data = Seq(
//      ("Thin", "Cell phone", 6000),
//      ("Normal", "Tablet", 1500),
//      ("Mini", "Tablet", 5500),
//      ("Ultra thin", "Cell phone", 5000),
//      ("Very thin", "Cell phone", 6000),
//      ("Big", "Tablet", 2500),
//      ("Bendable", "Cell phone", 3000),
//      ("Foldable", "Cell phone", 3000),
//      ("Pro", "Tablet", 4500),
//      ("Pro2", "Tablet", 6500)
//    )
//    val source: DataFrame = data.toDF("product", "category", "revenue")
//    import org.apache.spark.sql.functions._
//
//
//    //  ==========================================================================
//    //  ========================  函数  =======================================
//    //  ==========================================================================
//
//    // -------------------  UTF函数  ---------------------
//    // 命令式
//    def toStr(revenue: Long): String = "$" + revenue // 1.定义UDF函数
//    val toStrUDF: UserDefinedFunction = udf(toStr _) // 2.注册UDF函数
//    source.select(toStrUDF('revenue)).show()
//
//    // sql方式
//    source.createOrReplaceTempView("table_1")
//    spark.udf.register("utf_func", (x: Int) => "$" + x)
//    spark.sql("select udf_str(revenue) from table_1").show()
//
//
//    // -------------------  窗口函数  ---------------------
//    /*
//      1.排名函数
//          row_number     不考虑数据重复性,依次连续打上标号 ==> [1 2 3 4]
//          dense_rank     考虑数据重复性,重复的数据不会挤占后续的标号,是连续的 ==> [1 2 2 3]
//          rank           排名函数,考虑数据重复性,重复的数据会挤占后续的标号 ==> [1 1 3 4]
//
//      2.分析函数
//          first          获取这个组第一条数据
//          last           获取这个组最后一条数据
//          lag            lag(field,n)获取当前数据的field列向前n条数据
//          lead           lead(field,n)获取当前数据的field列向后n条数据
//
//      3.聚合函数
//          sum/avg..      所有的functions中的聚合函数都支持
//     */
//
//
//    // 1.定义窗口规则
//    import org.apache.spark.sql.functions._
//    val window: WindowSpec = Window.partitionBy('category).orderBy('revenue.desc)
//
//    // 2.定义窗口函数
//    source.select('product, 'category, 'revenue, row_number() over window as "num").show()
//    source.withColumn("num", row_number() over window).show()
//
//
//  }
//
//}
//
