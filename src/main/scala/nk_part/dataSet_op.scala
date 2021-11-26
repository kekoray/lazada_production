//package nk_part
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
//import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
//
///*
// *
// * @ProjectName: lazada_production
// * @program:
// * @FileName: dataSet_op
// * @description:  TODO
// * @version: 1.0
// * *
// * @author: koray
// * @create: 2021-10-20 11:51
// * @Copyright (c) 2021,All Rights Reserved.
// */ object dataSet_op {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    val spark = SparkSession.builder().appName("dataSet_op").master("local[*]").getOrCreate()
//
//
//    //===================================================================================
//    //===========================   Dataset的常用操作   ==================================
//    //===================================================================================
//    import spark.implicits._
//
//
//    //===============  Dataset的有类型操作   ========================
//    //不能直接拿到列进行操作,需要通过.的形式获取
//
//    /*
//    1.转换
//        flatMap        处理对象是数据集中的每个元素,将一条数据转为一个数组
//        map            处理对象是数据集中的每个元素,将一条数据转为另一种形式
//        mapPartitions  处理对象是数据集中每个分区的iter迭代器
//        transform      处理对象是整个数据集(Dataet),可以直接拿到Dataset的API进行操作
//        as             类型转换
//    */
//
//    Seq("hello world", "hello pc").toDS().flatMap(_.split(" ")).show
//    Seq(1, 2, 3, 4).toDS().map(_ * 10).show
//    Seq(1, 2, 3, 4).toDS().mapPartitions(iter => iter.map(_ * 10)).show
//    spark.range(5).transform(dataset => dataset.withColumn("doubled", 'id * 2))
//    Seq("jark", "18").toDS().as[Person]
//
//
//    /*
//     2.过滤
//        filter                按照条件过滤数据
//
//     3.分组聚合
//        groupByKeygrouByKey   算子的返回结果是KeyValueGroupedDataset,而不是一个Dataset,
//                              所以必须要先经过KeyValueGroupedDataset中的方法进行聚合,再转回Dataset,才能使用Action得出结果;
//     4.切分
//       randomSplit            randomSplit会按照传入的权重随机将一个Dataset分为多个Dataset;数值的长度决定切分多少份;数组的数值决定权重
//       sample                 sample会随机在Dataset中抽样.
//
//     5.排序
//       orderBy                指定多个字段进行排序,默认升序排序
//       sort                   orderBy是sort的别名,功能是一样
//
//     6.去重
//       dropDuplicates   根据指定的列进行去重;不传入列名时,是根据所有列去重
//       distinct         dropDuplicates()的别名,根据所有列去重
//    */
//
//    Seq(1, 2, 3).toDS().filter(_ > 2).show
//
//    val result: KeyValueGroupedDataset[Int, (Int, String)] = Seq((1, "k"), (2, "w"), (1, "p")).toDS().groupByKey(_._1)
//    result.count().show
//
//    spark.range(15).randomSplit(Array[Double](2, 3)).foreach(_.show)
//    spark.range(15).sample(false, 0.5).show
//    Seq(Person("zhangsan", 12), Person("lisi", 15)).toDS().orderBy('age.asc).show
//    Seq(Person("zhangsan", 12), Person("lisi", 15)).toDS().sort('age.desc).show
//    Seq(Person("zhangsan", 12), Person("lisi", 15)).toDS().dropDuplicates("name").show
//    Seq(Person("zhangsan", 12), Person("lisi", 15)).toDS().distinct().show
//
//
//    /*
//     7.分区
//       coalesce         只能减少分区,会直接创建一个逻辑操作,并且设置Shuffle=false; 与RDD中coalesce不同
//       repartitions     作用是一个是重分区到特定的分区数,另一个是按照某一列来分区; 类似于SQL中的distribute by
//
//     8.集合操作
//       except      求得两个集合的差集
//       intersect   求得两个集合的交集
//       union       求得两个集合的并集
//       limit       限制结果集数量
//     */
//
//    spark.range(15).coalesce(1).explain(true)
//    spark.range(15).repartition(2).explain(true)
//
//    val ds1 = spark.range(1, 10)
//    val ds2 = spark.range(5, 15)
//    ds1.except(ds2).show
//    ds1.intersect(ds2).show
//    ds1.union(ds2).show
//    ds1.limit(3).show
//
//
//
//
//
//    //===============  Dataset的无类型转换操作   ====================
//    // 可直接拿到列进行操作,使用函数功能的话就要导入隐式转换
//    import org.apache.spark.sql.functions._
//
//    /*
//     1.选择
//         select              用于选择某些列出现在结果集中
//         selectExpr          使用expr函数的形式选择某些列出现在结果集中
//
//     2.列操作
//         withColumn          创建一个新的列或者修改原来的列
//         withColumnRenamed   修改列名
//
//     3.剪除
//         drop                删掉某个列
//
//     4.聚合
//         groupBy             按照给定的行进行分组
//     */
//    val peopleDS: Dataset[Person] = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
//    peopleDS.select(expr("count(age) as count")).show
//    peopleDS.selectExpr("count(age) as count").show
//    peopleDS.withColumn("random", expr("rand()")).show
//    peopleDS.withColumnRenamed("name", "new_name").show
//    peopleDS.drop('age).show
//    peopleDS.groupBy('name).count().show
//
//
//
//    //===============  Column对象  ================================
//
//    // ---------------  column创建方式  -------------------
//    import org.apache.spark.sql.functions._ // 作用于col,column
//    import spark.implicits._ // 作用于符号',$
//
//    val dataSet: Dataset[Person] = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()
//
//    // 创建Column对象
//    dataSet
//      .select('name) // 常用
//      .select($"name")
//      .select(col("name"))
//      .select(column("name"))
//      .where('age > 0)
//      .where("age > 0")
//
//    // 创建关联此Dataset的Column对象
//    dataSet.col("addCol")
//    dataSet.apply("addCol2")
//    dataSet("addCol2")
//
//
//    // ---------------  column常用操作  -----------------
//    // 1.类型转换
//    dataSet.select('age.as[String])
//
//    // 2.创建别名
//    dataSet.select('name.as("other_name"))
//
//    // 3.添加列
//    dataSet.withColumn("double_age", 'age * 2)
//
//    // 4.模糊查找
//    dataSet.select('name.like("apple"))
//
//    // 5.是否存在指定列
//    dataSet.select('name.isin("a", "b"))
//
//    // 6.正反排序
//    dataSet.sort('age.asc)
//    dataSet.sort('age.desc)
//
//
//
//
//
//  }
//
//  case class Person(name: String, age: Int)
//
//}
