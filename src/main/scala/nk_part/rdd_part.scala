package nk_part

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/*
 * 
 * @ProjectName: lazada_production  
 * @program:    
 * @FileName: nk_part.rdd_part
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-09-09 11:53  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object rdd_part {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("nk_part.rdd_part").setMaster("local[2]")
    val sc = new SparkContext(conf)


    //    // =======================================================================
    //    // ===========================  RDD创建  ================================
    //    // ***********************************************************************
    //
    //    // 1.通过本地集合直接创建,使用parallelize和makeRDD
    //    val seq = Seq(1, 2, 3, 4, 5)
    //    val rdd1: RDD[Int] = sc.parallelize(seq, 2)
    //    val rdd2: RDD[Int] = sc.makeRDD(seq, 2)
    //    rdd1.foreach(x => print(s"rdd1 : ${x}   "))
    //    rdd2.foreach(x => print(s"rdd2 : ${x}   "))
    //
    //    // 2.通过读取外部数据集来创建
    //    /* a.访问本地文件,sc.textFile("file:///…")
    //       b.访问其他系统的文件,sc.textFile("hdfs://node-1:8020/dataset")   */
    //    val source: RDD[String] = sc.textFile("src/main/resources/wordcount.txt", 2)
    //    source.foreach(x => println(s"source : ${x}   "))
    //
    //    // 3.通过其它的RDD衍生而来
    //    val rdd3: RDD[String] = source.flatMap(_.split(" "))
    //    rdd3.foreach(x => print(s"rdd3 : ${x}   "))
    //
    //
    //
    // =======================================================================
    // ===========================  RDD转换算子  ===============================
    // ***********************************************************************

    // ----------------  转换  -------------------------
    // map -- 针对每一条数据中的每个元素进行转换
    sc.makeRDD(Seq(1, 2, 3, 4))
      .map(_ * 10)
      .foreach(println)

    // mapPartitions -- 针对整个分区中的每一条数据进行转换
    sc.makeRDD(Seq(1, 2, 3, 4), 5)
      .mapPartitions(iter => {
        // iter是scala中的集合类型,遍历iter其中每一条数据进行转换,转换完成以后,返回这个iter
        iter.map(item => item * 10)
      })
      .foreach(println)

    // mapValues -- 只能作用于Key-Value型数据的Value中
    sc.makeRDD(Seq(("a", 1), ("b", 2), ("c", 3)))
      .mapValues(x => x * 10)
      .foreach(println)

    // mapPartitionsWithIndex -- 用于获取每个分区的数据
    sc.makeRDD(Seq(1, 2, 3, 4), 5)
      .mapPartitionsWithIndex((index, iter) => {
        println(s"index : ${index}  ---  ${iter.mkString(":")}")
        iter
      })
      .foreach(println)

    // flatmap -- 返回的是经过函数转换成的新RDD集合
    sc.makeRDD(Seq("Hello lily", "Hello lucy", "Hello tim"))
      .flatMap(_.split(" "))
      .foreach(println)



    // ----------------  过滤与排序  -------------------------
    // filter -- 过滤掉数据集中一部分元素
    sc.makeRDD(Seq(1, 2, 3, 4))
      .filter(_ > 3)
      .foreach(println)

    // Sample -- 从一个数据集中抽样出来一部分, 常用作于把大数据集变小, 尽可能的减少数据集规律的损失.
    sc.makeRDD(Seq(1, 2, 3, 4))
      .sample(withReplacement = true, 0.4)
      .foreach(println)

    // sortBy -- 适用于任何类型RDD,按照任意部分排序
    sc.makeRDD(Seq(1, 2, 3, 4))
      .sortBy(x => x, ascending = false)
      .foreach(println)

    // ortByKey -- 只适用于KV类型RDD,按照key排序
    sc.makeRDD(Seq(("a", 1), ("b", 3), ("c", 2)))
      .sortByKey(ascending = false)
      .foreach(println)


    // ----------------  集合  -------------------------
    val rdd1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("a", 11), ("a", 12)))
    val rdd3 = sc.parallelize(Seq(("zhangsan", 99.0), ("zhangsan", 96.0), ("lisi", 97.0), ("lisi", 98.0), ("zhangsan", 97.0)))
    val rdd4 = sc.parallelize(Seq(("zhangsan", 22.0), ("zhangsan", 35.0), ("lisi", 97.0), ("lisi", 98.0), ("zhangsan", 97.0)))

    //
    rdd1.union(rdd2).foreach(println)
    rdd1.intersection(rdd2).foreach(println)
    rdd1.subtract(rdd2).foreach(println)
    rdd1.subtract(rdd2).foreach(println)
    rdd1.join(rdd2).foreach(println)
    rdd1.distinct().foreach(println)
    rdd1.reduceByKey((x, y) => x + y).foreach(println)
    rdd1.groupByKey().foreach(println)


    // combineByKey
    type RDDType = (Double, Int) // type用于封装数据类型为(Double, Int)在变量RDDType里

    rdd3.combineByKey(
      createCombiner = (curr: Double) => (curr, 1),
      mergeValue = (curr: RDDType, nextValue: Double) => (curr._1 + nextValue, curr._2 + 1),
      mergeCombiners = (curr: RDDType, agg: RDDType) => (curr._1 + agg._1, curr._2 + agg._2))
      .map(x => (x._1 -> x._2._1 / x._2._2)) // 分组后求求平均
      .foreach(println)

    // aggregateByKey
    rdd3.aggregateByKey(0.5)(
      seqOp = (zero, price) => price * zero,
      combOp = (curr, agg) => curr + agg
    ).foreach(println)

    rdd3.foldByKey(10)((curr, agg) => curr + agg).foreach(println)

    rdd3.cogroup(rdd4).foreach(println)

    // ----------------  重分区  -------------------------


    // flatmap
    sc.makeRDD(Seq("Hello lily", "Hello lucy", "Hello tim"))
      .flatMap(_.split(" "))
      .foreach(println)

    // filter
    sc.makeRDD(Seq(1, 2, 3))
      .filter(_ > 2)
      .foreach(println)




    // mapPartitionsWithIndex
    // mapValues
    // sample
    // union
    // intersection
    // subtract
    // distinct


    // reducebykey
    sc.makeRDD(Seq(("a", 1), ("a", 1), ("b", 1)))
      .reduceByKey((x, y) => x + y)
      .foreach(println)

    // groupByKey
    // combineByKey
    // aggregateByKey
    // foldByKey
    // join
    // cogroup
    // cartesian
    // sortBy
    // partitionBy
    // coalesce
    // repartition
    // repartitionAndSortWithinPartitions


    // =======================================================================
    // ===========================  RDD动作算子  ===============================
    // ***********************************************************************

    //    // reduce
    //    // collect
    //    // count
    //    // first
    //    // take
    //    // takeSample
    //    // fold
    //    // saveAsTextFile
    //    // saveAsSequenceFile
    //    // countByKey
    //    // foreach
    //
    //
    //    // =======================================================================
    //    // ===========================  RDD分区操作  ================================
    //    // ***********************************************************************
    //
    //
    //    // 查看分区数
    //    println(sc.makeRDD(Seq(1, 2, 3, 4)).map(_ * 10).partitions.size)
    //
    //    // 1.创建RDD时指定分区数
    //    println(sc.makeRDD(Seq(1, 2, 3, 4), 10).partitions.size) // 10
    //    // 2.通过coalesce算子指定
    //    println(sc.makeRDD(Seq(1, 2, 3, 4), 10).coalesce(5, shuffle = false).partitions.size) // 5
    //    // 3.通过repartition算子指定
    //    println(sc.makeRDD(Seq(1, 2, 3, 4), 10).repartition(3).partitions.size) // 3
    //
    //
    //    // =======================================================================
    //    // ===========================  RDD容错  ================================
    //    // ***********************************************************************
    //
    //
    //    //----------------  缓存  ----------------
    //    val rdd = sc.makeRDD(Seq("a", "b", "c"))
    //      .map((_, 1))
    //      .reduceByKey((x, y) => x + y)
    //
    //    // cache等同于persist() ==> persist(StorageLevel.MEMORY_ONLY)
    //    rdd.cache()
    //
    //    // persist能够指定缓存的级别
    //    rdd.persist(StorageLevel.MEMORY_ONLY)
    //
    //    // 清理缓存
    //    rdd.unpersist()
    //
    //    //----------------  Checkpoint  ----------------
    //
    //    // 1.先设置Checkpoint的存储路径
    //    sc.setCheckpointDir("checkpoint")
    //
    //    // 2.开启Checkpoint
    //    rdd.checkpoint()
    //
    //
    //    // =======================================================================
    //    // ===========================  分布式变量  ================================
    //    // ***********************************************************************
    //
    //
    //    //----------------  全局累加器  ----------------
    //    // 支持数值型累加add()的分布式变量,默认值为0,遇到action算子触发
    //    val counter = sc.longAccumulator("counter")
    //    sc.makeRDD(Seq(1, 2, 3, 4)).foreach(counter.add(_))
    //    //    print(counter.value)
    //
    //
    //    //----------------  自定义累加器  ----------------
    //    val infoAccumulator = new InfoAccumulator()
    //    // 注册自定义累加器
    //    sc.register(infoAccumulator, "infos")
    //    sc.makeRDD(Seq("1", "2", "3", "4")).foreach(infoAccumulator.add)
    //    //    print(counter.value)
    //
    //
    //    //----------------  广播变量  ----------------
    //    // 1.创建广播变量
    //    val a = sc.broadcast(1)
    //    // 2.获取值
    //    println(a.value)
    //    // 3.销毁变量,释放内存空间
    //    a.destroy()
    //
    //    // 唯一标识
    //    println(a.id)
    //    // 字符串表示
    //    println(a.toString())
    //    // 在Executor中异步的删除缓存副本
    //    a.unpersist()
    //

  }

}


// ============================================================================
// ============================================================================
// ============================================================================

// 自定义累加器,继承AccumulatorV2,第一个参数是传入类型,第二个是输出类型
class InfoAccumulator extends AccumulatorV2[String, Set[String]] {

  // 创建可变集合用于收集累加值
  private val infos: mutable.Set[String] = mutable.Set()

  // 初始化累加器对象是否为空
  override def isZero: Boolean = {
    infos.isEmpty
  }

  // 拷贝创建一个新累加器对象
  override def copy(): AccumulatorV2[String, Set[String]] = {
    val infoAccumulator = new InfoAccumulator()
    infos.synchronized {
      infoAccumulator.infos ++= infos
    }
    infoAccumulator
  }

  // 重置累加器数据
  override def reset(): Unit = {
    infos.clear()
  }

  // 外部传入要累加的内容,在这个方法中进行累加
  override def add(v: String): Unit = {
    infos += v
  }

  // 累加器在进行累加的时候,可能每个分布式节点都有一个实例,在最后Driver端进行一次合并,把所有的实例的内容合并起来
  override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
    infos ++= other.value
  }

  // 提供给外部累加的结果
  override def value: Set[String] = {
    //需要返回一个不可变的集合,因为不能因为外部的修改而影响自身的值
    infos.toSet
  }
}
