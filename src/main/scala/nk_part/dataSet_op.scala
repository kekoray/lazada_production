package nk_part

import org.apache.spark.sql.SparkSession

/*
 * 
 * @ProjectName: lazada_production  
 * @program:
 * @FileName: dataSet_op
 * @description:  TODO
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2021-10-20 11:51
 * @Copyright (c) 2021,All Rights Reserved.
 */ object dataSet_op {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataSet_op").master("local[*]").getOrCreate()
    import spark._


    //===================================================================================
    //===========================   Dataset的常用操作   ==================================
    //===================================================================================


    //===============  Dataset的有类型操作   ========================
    //不能直接拿到列进行操作,需要通过.的形式获取

    /*
    1.转换
        flatMap        处理对象是数据集中的每个元素
        map            处理对象是数据集中的每个元素
        mapPartitions  处理对象是数据集中每个分区的iter迭代器
        transform      处理对象是整个数据集 (Dataset) ;   transform 可以直接拿到Dataset进行操作
        as             作用是将弱类型的 Dataset 转为强类型的 Dataset ;  转成Dataset方便于转换操作

     2.过滤
        filter  按照条件过滤数据集

     3.分组聚合
        groupByKeygrouByKey  算子的返回结果是KeyValueGroupedDataset,而不是一个Dataset,
                             所以必须要先经过KeyValueGroupedDataset中的方法进行聚合,再转回Dataset,才能使用Action得出结果;

     4.切分
       randomSplit  randomSplit会按照传入的权重随机将一个Dataset分为多个Dataset;数值的长度决定切分多少份;数组的数值决定权重
       sample       sample会随机在Dataset中抽样.

     5.排序
       orderBy   指定多个字段进行排序,默认升序排序
       sort      orderBy是sort的别名,功能是一样的

     6.去重
       dropDuplicates   根据指定的列进行去重;不传入列名时,是根据所有列去重
       distinct         dropDuplicates()的别名,根据所有列去重

     7.集合操作
       except      求得两个集合的差集
       intersect   求得两个集合的交集
       union       求得两个集合的并集
       limit       限制结果集数量

     */






    //===============  Dataset的无类型转换操作   ====================
    // 可直接拿到列进行操作,使用函数功能的话就要导入隐式转换
    import org.apache.spark.sql.functions._

    /*
1.查询
select      用于选择某些列出现在结果集中
selectExpr  以SQL表达式的形式选择某些列出现在结果集中



     */





    //===============  Column对象   ================================

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


  }

}
