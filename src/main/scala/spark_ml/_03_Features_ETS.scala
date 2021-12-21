package spark_ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{Binarizer, Bucketizer, IndexToString, MaxAbsScaler, MinMaxScaler, OneHotEncoder, StandardScaler, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_ml   
 * @FileName: _03_Features_ETS 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-21 11:51  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object _03_Features_ETS {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().appName("_03_Features_ETS").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    //=================  1.Feature Extractors  ==================
    /*
    特征抽取,主要是针对文本数据、图像数据、视频数据等需要结构化数据的抽取;
    一般我们都是处理MySQL中结构化的数据;
    在sparkMllib中仅提供了文本抽取的API;
     */

    //=========================================================
    //=================  2.Feature Transformers  ==================
    //=========================================================
    /*
     特征转化
     1.类别型数据的数值化
           (a).标签编码  -->  StringIndexer,IndexToString
           (b).独热编码  -->  OneHotEncoder
     2.数值型数据的归一化和标准化
          (a).归一化  -->  MinMaxScaler,MaxAbsScaler
          (b).标准化  -->  StandardScaler
     3.连续值数据的离散化
          (a).二值化操作  -->  Binarizer
          (b).分桶操作    -->  Bucketizer
     4.特征组合
          (a).向量汇编器  -->  VectorAssembler

     */

    /* =================  1.类别型数据的数值化  ================
        (a).labelencoder标签编码---如果原始数据是有顺序的情况下,使用StringIndexer实现,如abc,123等;
        (b).onehotencoder独热编码---如果原始数据没有顺序的,使用OneHotEncoder实现,如男女,大小等;
     */

    val data = spark.createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))).toDF("id", "category")

    // -------------- (a).标签编码 ------------------
    // 使用StringIndexer将类别型数据转换为以标签索引为主的数值型数据,索引在[0,numLabels)中,按标签频率排序,最频繁的标签为0索引;
    // setInputCol() 输入列,需要转化的schema
    // setOutputCol() 输出列,用户自定义
    val indexer: StringIndexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndexer")
    // 在特征工程中需要fit形成model在使用tranforam进行转化,model保存了数据转化前后的映射关系
    val indexerModel: StringIndexerModel = indexer.fit(data)
    val indexerResultDF: DataFrame = indexerModel.transform(data)
    indexerResultDF.show(false)
    /*
          +---+--------+---------------+
          |id |category|categoryIndexer|
          +---+--------+---------------+
          |0  |a       |0.0            |
          |1  |b       |2.0            |
          |2  |c       |1.0            |
          |3  |a       |0.0            |
          |4  |a       |0.0            |
          |5  |c       |1.0            |
          +---+--------+---------------+
     */

    // 对预测值进行原数据映射
    // setInputCol() 输入列,需要映射预测的schema
    // setOutputCol() 输出列,用户自定义
    // setLabels() 映射关系,model保存了数据转化前后的映射关系
    val indexToString: IndexToString = new IndexToString().setInputCol("categoryIndexer").setOutputCol("beforeIndex").setLabels(indexerModel.labels)
    val indexToStringResult: DataFrame = indexToString.transform(indexerResultDF)
    indexToStringResult.show(false)
    /*
        +---+--------+---------------+-----------+
        |id |category|categoryIndexer|beforeIndex|
        +---+--------+---------------+-----------+
        |0  |a       |0.0            |a          |
        |1  |b       |2.0            |b          |
        |2  |c       |1.0            |c          |
        |3  |a       |0.0            |a          |
        |4  |a       |0.0            |a          |
        |5  |c       |1.0            |c          |
        +---+--------+---------------+-----------+
     */

    // -------------- (b).独热编码 ------------------
    // 将一列类别索引映射到一列二进制向量,每行最多有一个单值,表示输入类别索引,不过已在spark2.3.0中弃用,并将在3.0.0中删除;
    // 首先要先将数据转化为标签索引数据,再使用独热编码;
    val encoder: OneHotEncoder = new OneHotEncoder().setInputCol("categoryIndexer").setOutputCol("oheIndex").setDropLast(false)
    val oheResult = encoder.transform(indexerResultDF)
    oheResult.show(false)
    /*
        +---+--------+---------------+-------------+
        |id |category|categoryIndexer|oheIndex     |
        +---+--------+---------------+-------------+
        |0  |a       |0.0            |(3,[0],[1.0])|  -->  (1.0, 0.0, 0.0)   <--  稀疏向量,只存非0值
        |1  |b       |2.0            |(3,[2],[1.0])|  -->  (0.0, 0.0, 1.0)
        |2  |c       |1.0            |(3,[1],[1.0])|  -->  (0.0, 1.0, 0.0)
        |3  |a       |0.0            |(3,[0],[1.0])|
        |4  |a       |0.0            |(3,[0],[1.0])|
        |5  |c       |1.0            |(3,[1],[1.0])|
        +---+--------+---------------+-------------+
     */


    // =================  2.数值型数据的归一化和标准化  ================
    /*
      (a).归一化操作,即在具备不同量纲的前提下,通过归一化操作能够将所有的数据归一化到[0,1]或[-1,1]的区间,从而降低因为量纲对模型带来的影响;
          MinMaxScaler: （当前的值-最小值）/（最大值-最小值),可以将数据归一化到[最小值,最大值]=[0,1]区间
          MaxAbsScaler:   (当前的值)/max(abs(这一列的取值)),可以将数据归一化到[-1,1]区间

      (b).标准化操作,因为某些算法需要数据呈现为标准正态分布,所以需要对数据进行标准化
          StandSclaer:  当前的值减去均值或者方差,适合于正态分布转化为标准正态分布的数据
     */

    val df3 = spark.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.5, -1.0)),
      (1, Vectors.dense(2.0, 1.0, 1.0)),
      (2, Vectors.dense(4.0, 10.0, 2.0))
    )).toDF("id", "features")

    // -------------- (a).归一化操作 ------------------
    val minMaxDF = new MinMaxScaler().setInputCol("features").setOutputCol("MinMaxfeatures").fit(df3).transform(df3)
    val maxAbsDF = new MaxAbsScaler().setInputCol("features").setOutputCol("MaxAbsfeatures").fit(df3).transform(df3)
    minMaxDF.show(false)
    maxAbsDF.show(false)
    /*
          +---+--------------+-----------------------------------------------------------+
          |id |features      |MinMaxfeatures                                             |
          +---+--------------+-----------------------------------------------------------+
          |0  |[1.0,0.5,-1.0]|[0.0,0.0,0.0]                                              |
          |1  |[2.0,1.0,1.0] |[0.3333333333333333,0.05263157894736842,0.6666666666666666]|
          |2  |[4.0,10.0,2.0]|[1.0,1.0,1.0]                                              |
          +---+--------------+-----------------------------------------------------------+

          +---+--------------+----------------+
          |id |features      |MaxAbsfeatures  |
          +---+--------------+----------------+
          |0  |[1.0,0.5,-1.0]|[0.25,0.05,-0.5]|
          |1  |[2.0,1.0,1.0] |[0.5,0.1,0.5]   |
          |2  |[4.0,10.0,2.0]|[1.0,1.0,1.0]   |
          +---+--------------+----------------+
          */


    // -------------- (b).标准化操作 ------------------
    val standardDF = new StandardScaler().setInputCol("features").setOutputCol("Standardfeatures").fit(df3).transform(df3)
    standardDF.show(false)
    /*
          +---+--------------+------------------------------------------------------------+
          |id |features      |Standardfeatures                                            |
          +---+--------------+------------------------------------------------------------+
          |0  |[1.0,0.5,-1.0]|[0.6546536707079771,0.09352195295828246,-0.6546536707079771]|
          |1  |[2.0,1.0,1.0] |[1.3093073414159542,0.18704390591656492,0.6546536707079771] |
          |2  |[4.0,10.0,2.0]|[2.6186146828319083,1.8704390591656492,1.3093073414159542]  |
          +---+--------------+------------------------------------------------------------+
     */


    // =================  3.连续值数据的离散化  ================
    /*
    离散化的原因是因为连续性数据是不符合某些算法的要求的,比如决策树;
       (a).二值化操作(Binarizer),只能划分成2种类别的数据
       (b).分桶操作(Bucketizer),可以划分成多种类别的数据
     */

    val df1 = spark.createDataFrame(Array((0, 0.1), (1, 8.0), (2, 0.2), (3, -2.0), (4, 0.0))).toDF("label", "feature")

    // -------------- (a).二值化操作 ------------------
    val binarizerDF = new Binarizer().setInputCol("feature").setOutputCol("feature_binarizer").setThreshold(0.5).transform(df1)
    binarizerDF.show(false)
    /*
        +-----+-------+-----------------+
        |label|feature|feature_binarizer|
        +-----+-------+-----------------+
        |0    |0.1    |0.0              |
        |1    |8.0    |1.0              |
        |2    |0.2    |0.0              |
        |3    |-2.0   |0.0              |
        |4    |0.0    |0.0              |
        +-----+-------+-----------------+
     */


    // -------------- (b).分桶操作 ------------------
    // 分箱条件(正无穷 -> 0 -> 10 -> 正无穷)
    val splits = Array(Double.NegativeInfinity, 0, 5, Double.PositiveInfinity)
    val bucketizerDF = new Bucketizer().setInputCol("feature").setOutputCol("feature_bucketizer").setSplits(splits).transform(df1)
    bucketizerDF.show(false)
    /*
        +-----+-------+------------------+
        |label|feature|feature_bucketizer|
        +-----+-------+------------------+
        |0    |0.1    |1.0               |
        |1    |8.0    |2.0               |
        |2    |0.2    |1.0               |
        |3    |-2.0   |0.0               |
        |4    |0.0    |1.0               |
        +-----+-------+------------------+
     */


    // =================  4.特征组合  ================
    // 特征组合就是将指定的多列的Array数组组合成一个向量列,并且输入列的值将按照指定的顺序组合成一个向量;
    val df2 = spark.createDataFrame(Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))).toDF("id", "hour", "mobile", "userFeatures", "clicked")
    val VectorAssemblerDF = new VectorAssembler().setInputCols(Array("hour", "mobile", "userFeatures", "clicked")).setOutputCol("features").transform(df2)
    VectorAssemblerDF.printSchema()
    VectorAssemblerDF.show(false)
    /*
        root
         |-- id: integer (nullable = false)
         |-- hour: integer (nullable = false)
         |-- mobile: double (nullable = false)
         |-- userFeatures: vector (nullable = true)
         |-- clicked: double (nullable = false)
         |-- features: vector (nullable = true)


        +---+----+------+--------------+-------+---------------------------+
        |id |hour|mobile|userFeatures  |clicked|features                   |
        +---+----+------+--------------+-------+---------------------------+
        |0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |[18.0,1.0,0.0,10.0,0.5,1.0]|
        +---+----+------+--------------+-------+---------------------------+
     */


    //=========================================================
    //=================  3.Feature Selectors  ==================
    //=========================================================

  }

}
