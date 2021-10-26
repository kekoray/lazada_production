package nk_part

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: nk_part   
 * @FileName: dataFarm_op 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-26 11:27  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object dataFrame_op {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark: SparkSession = SparkSession.builder().appName("dataFrame_op").master("local[*]").getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._


    //  =======================  缺失值处理  =======================
    /*
     常见的缺失值有两种
         1. Double.NaN类型的NaN和字符串对象null等特殊类型的值,也是空对象,一般使用drop,fill处理
         2. "Null", "NA", " "等为字符串类型的值,一般使用replace处理
     */

    val df: DataFrame = List((1, "kk", 18.0), (2, "", 25.0), (3, "UNKNOWN", Double.NaN), (4, "null", 30.0)).toDF("id", "name", "age")

    // 删除操作,删除包含null和NaN的行,可以指定列;
    // [ any ==> 处理的是当某行数据任意一个字段为空, all ==> 处理的是当某行数据所有值为空 ]
    //1.默认为any,当某一列有NaN时就删除该行
    df.na.drop().show()
    df.na.drop("any").show()
    df.na.drop("any", List("id", "name")).show()

    //2.all,所有的列都是NaN时就删除该行
    df.na.drop("all").show()


    // 填充操作,对包含null和NaN的所有列数据进行默认值填充,可以指定列;
    df.na.fill(0).show()
    df.na.fill(0, List("id", "name")).show()

    // 替换操作,替换包含'字符串类型的缺省值'的所有列数据为其它值,被替换的值和新值必须是同类型,可以指定列;
    //1.将“名称”列中所有出现的“UNKNOWN”替换为“unnamed”
    df.na.replace("name", Map("UNKNOWN" -> "unnamed")).show()

    //2.在所有字符串列中将所有出现的“ UNKNOWN”替换为“ unnamed"
    df.na.replace("*", Map("UNKNOWN" -> "unnamed")).show()


    //  ========================  多维分组聚合操作  =======================================
    /*
    1.单维聚合 group by
    2.多维聚合 roll,cube

     */

    val sales = Seq(
      ("Beijing", 2016, 100),
      ("Beijing", 2017, 200),
      ("Shanghai", 2015, 50),
      ("Shanghai", 2016, 150),
      ("Guangzhou", 2017, 50)
    ).toDF("city", "year", "amount")

    val groupedDF: RelationalGroupedDataset = sales.groupBy('city, 'year)

    groupedDF
      .agg(sum("amount") as "sum_amount")
      .sort('year asc_nulls_last)
      .select('city, 'sum_amount)
      .show()


    //  ========================  连接操作  =======================================

    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0)).toDF("id", "name", "cityId")
    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou")).toDF("id", "name")

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

    // ---------------- Map端Join操作 -------------
    /*
    将小数据集分发给每一个Executor,然后在需要Join的时候,让较大的数据集在Map端直接获取小数据集进行Join,这种方式是不需要进行Shuffle的,所以称之为Map端Join操作
    使用Dataset可以自动实现Map端Join,需要设置spark.sql.autoBroadcastJoinThreshold参数,当数据集小于这个参数的大小时,会自动进行Map端Join操作

   */
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt / 1024 / 1024)
    println(person.crossJoin(cities).queryExecution.sparkPlan.numberedTreeString)
    // 关闭
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    //  ========================  窗口函数  =======================================


  }

}
