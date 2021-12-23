package spark_ml.kn

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 *
 * @ProjectName: lazada_production
 * @program: spark_ml
 * @FileName: _01_veter
 * @description:  TODO
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2021-12-03 18:36
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _01_vector {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_01_vector").master("local[6]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    /*
        SparkMllIb数据类型简介
        MLLIB支持很多种机器学习算法中类型,主要有向量和矩阵两种类型;
            (1) Local vector本地向量集,主要向Spark提供一组可进行操作的数据集合
            (2) Labeled Point向量标签,主要用于在算法中做标记,在分类问题中,可以将不同的数据集分成若干份,以整数型0、1、2进行标记;
            (3) Local matrix本地矩阵,将数据集合以矩阵形式存储在本地计算机中
            (4) Distribute matrix分布式矩阵,在本地矩阵的基础上增加了分布式特性

       在MLlib的数据支持格式中,目前仅支持整数与浮点型数,主要是用于做数值计;


    */

    // 1.构建本地向量集
    // (1)密集型向量,可存储0值和非0值
    val v1: linalg.Vector = Vectors.dense(firstValue = 4.0, otherValues = 0.0, 2.0, 5.0) // [4.0,0.0,2.0,5.0]

    // (2)稀疏型向量,仅仅存储非0值的下标和非0只对应的值,也就是说0值的下标和值都不做存储的,能节省很大的空间
    val v2: linalg.Vector = Vectors.sparse(size = 4, elements = Seq((1, 1.0), (3, 9.0))) // (4,[1,3],[1.0,9.0]) ==> 4个元素,其中索引1对应的是1.0,索引3对应的是9.0,其他索引对应的值都是0
    val v3: linalg.Vector = Vectors.sparse(size = 4, indices = Array(1, 2), values = Array(1.0, 9.0)) // (4,[1,2],[1.0,9.0]) ==> 4个元素,索引数组为1和2,对应的值数组为1.0和9.0,其他索引对应的值都是0
    //    println(v2(2)) // 0


    // 2.构建向量标签
    val labeledPoint = new LabeledPoint(label = 1.0, features = v1)
    //    println(labeledPoint) // (1.0,[4.0,0.0,2.0,5.0])
    //    println(labeledPoint.label) // 1.0
    //    println(labeledPoint.features) // [4.0,0.0,2.0,5.0]

    // libsvm的格式数据
    /*  libsvm的格式仅存储非0值和下标:
            label index:value index:Value index:value
            1 1:-0.555556 2:0.25 3:-0.864407 4:-0.916667
            1 1:-0.666667 2:-0.166667 3:-0.864407 4:-0.916667
            1 1:-0.777778 3:-0.898305 4:-0.916667
            1 1:-0.833333 2:-0.0833334 3:-0.830508 4:-0.916667
            1 1:-0.611111 2:0.333333 3:-0.864407 4:-0.916667
     */

    // spark-core
    val irisData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "src/main/resources/ML_data/iris.scale")
    //    irisData.take(20).foreach(println(_))
    /*  (1.0,(4,[0,1,2,3],[-0.555556,0.25,-0.864407,-0.916667]))
        (1.0,(4,[0,1,2,3],[-0.666667,-0.166667,-0.864407,-0.916667]))
        (1.0,(4,[0,2,3],[-0.777778,-0.898305,-0.916667]))
        (1.0,(4,[0,1,2,3],[-0.833333,-0.0833334,-0.830508,-0.916667]))
        (1.0,(4,[0,1,2,3],[-0.611111,0.333333,-0.864407,-0.916667]))    */


    // spark-sql
    val irisDF: DataFrame = spark.read.format("libsvm").load("src/main/resources/ML_data/iris.scale")
    //    irisDF.printSchema()
    /*  root
         |-- label: double (nullable = true)
         |-- features: vector (nullable = true)       */
    //    irisDF.show(5, truncate = false)
    /*   +-----+--------------------------------------------------------+
        |label|features                                                |
        +-----+--------------------------------------------------------+
        |1.0  |(4,[0,1,2,3],[-0.555556,0.25,-0.864407,-0.916667])      |
        |1.0  |(4,[0,1,2,3],[-0.666667,-0.166667,-0.864407,-0.916667]) |
        |1.0  |(4,[0,2,3],[-0.777778,-0.898305,-0.916667])             |
        |1.0  |(4,[0,1,2,3],[-0.833333,-0.0833334,-0.830508,-0.916667])|
        |1.0  |(4,[0,1,2,3],[-0.611111,0.333333,-0.864407,-0.916667])  |
        +-----+--------------------------------------------------------+   */


    // 3.构建本地矩阵
    val matrix1 = Matrices.dense(numRows = 2, numCols = 3, values = Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
    //    print(matrix1)
    /*  1.0  3.0  5.0
        2.0  4.0  6.0   */

    // 4.分布式矩阵
    val v4: RDD[linalg.Vector] = sc.makeRDD(Seq("1 2 3", "4 5 6")).map(_.split(" ").map(_.toDouble)).map(x => Vectors.dense(x))
    val rowMatrix: RowMatrix = new RowMatrix(v4)
    println(rowMatrix) // org.apache.spark.mllib.linalg.distributed.RowMatrix@448ade1
    println(rowMatrix.numRows()) // 2
    println(rowMatrix.numCols()) // 3


    spark.stop()
    sc.stop()


  }

}
