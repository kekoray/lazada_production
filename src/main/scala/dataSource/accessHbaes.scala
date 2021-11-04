package dataSource

import java.net.URI
import java.util.UUID

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Put, Result, Table, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

import scala.collection.mutable.ListBuffer
import scala.util.Try


/*
 *
 * @ProjectName: lazada_production
 * @program: dataSource
 * @FileName: accessHbaes
 * @description:  TODO
 * @version: 1.0
 * *
 * @author: koray
 * @create: 2021-10-18 15:12
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessHbaes {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("accessHbase").master("local[*]")
      // .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    // hbase的配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.216")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //在IDE中设置此项为true，避免出现"hbase-default.xml"版本不匹配的运行时异常
    hbaseConf.set("hbase.defaults.for.version.skip", "true")



    // =======================  管理操作  =======================
    // 获取admina管理员
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin

    // 查表
    println(admin.listTableNames().toList)

    // 删除表
    admin.deleteTable(TableName.valueOf("table_name"))



    // =======================  读取数据  =======================

    // 设置查询的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "person")
    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()

    // 遍历结果
    hBaseRDD.map({ case (_, result) =>
      // 获取行键
      val key = Bytes.toString(result.getRow)
      // 通过列族和列名获取值
      val name = Bytes.toString(result.getValue("name".getBytes, "china".getBytes))
      (key, name)
    }).toDF("id", "name").show(false)



    // =======================  写入数据  =======================

    // ---------------------  saveAsNewAPIHadoopDataset-AP方式插入  --------------------
    val table_name = "person3"

    // 判断表是否存在,不存在则创建表
    if (!admin.tableExists(TableName.valueOf(table_name))) {
      val table_desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(table_name))
      //指定列簇,不需要创建列
      val col_desc = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).build()
      table_desc.setColumnFamily(col_desc)
      admin.createTable(table_desc.build())
    }

    // 设置写入的表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table_name)
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val data = sc.makeRDD(Array("1,Jack,M,26", "2,Rose,M,17")).map(_.split(","))
    val dataRDD = data.map(arr => {
      // 设置行健
      val put = new Put(Bytes.toBytes(arr(0)))
      // 设置列族和列名和值
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2)))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3).toInt))
      (new ImmutableBytesWritable, put)
    })

    // 数据写入hbase
    dataRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)


    // ---------------------  BulkLoad方式批量插入  --------------------
    /*
        BulkLoad原理是先利用mapreduce在hdfs上生成相应的HFlie文件,然后再把HFile文件导入到HBase中,以此来达到高效批量插入数据
        如果hdfs使用9000端口,会报错 [Protocol message end-group tag did not match expected tag]
      */

    val table_name = "person5"
    val table: Table = hbaseConn.getTable(TableName.valueOf(table_name))

    // 判断表是否存在,不存在则创建表
    if (!admin.tableExists(TableName.valueOf(table_name))) {
      val table_desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(table_name))
      //指定列簇,不需要创建列
      val col_desc = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).build()
      table_desc.setColumnFamily(col_desc)
      admin.createTable(table_desc.build())
    }

    // 设置写入的表名
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, table_name)
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, hbaseConn.getRegionLocator(TableName.valueOf(table_name)))

    // 生成HFlie文件
    val data = sc.makeRDD(Array("1,Jack,26", "2,Rose,17")).map(_.split(","))
    val dataRDD: RDD[(ImmutableBytesWritable, KeyValue)] = data.map(x => (DigestUtils.md5Hex(x(0)).substring(0, 3) + x(0), x(1), x(2)))
      .sortBy(_._1)
      .flatMap(x => {
        val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
        val kv1: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(x._2 + ""))
        val kv2: KeyValue = new KeyValue(Bytes.toBytes(x._1), Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(x._3 + ""))
        listBuffer.append((new ImmutableBytesWritable, kv2))
        listBuffer.append((new ImmutableBytesWritable, kv1))
        listBuffer
      })

    // 判断hdfs上文件是否存在，存在则删除
    val filePath = "hdfs://192.168.100.216:8020/tmp/hbaseBulk"
    val output: Path = new Path(filePath)
    val hdfs: FileSystem = FileSystem.get(URI.create(filePath), new Configuration())
    if (hdfs.exists(output)) {
      hdfs.delete(output, true)
    }

    // 数据写入hbase
    dataRDD.saveAsNewAPIHadoopFile(filePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path(filePath), admin, table, hbaseConn.getRegionLocator(TableName.valueOf(table_name)))
    hdfs.close()
    table.close()


    // ---------------------  Phoenix方式插入  --------------------
    // (测试操作没过,报错)

    //spark读取phoenix返回DataFrame的第一种方式
    val rdf = spark.read
      .format("jdbc")
      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      .option("url", "jdbc:phoenix:192.168.100.216:2181")
      .option("dbtable", "person5")
      .load()

    val rdfList = rdf.collect()
    for (i <- rdfList) {
      println(i.getString(0) + " " + i.getString(1) + " " + i.getString(2))
    }
    rdf.printSchema()


    //spark读取phoenix返回DataFrame的第二种方式
    val df = spark.read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "person5", "zkUrl" -> "192.168.100.216:2181"))
      .load()

    df.printSchema()
    val dfList = df.collect()
    for (i <- dfList) {
      println(i.getString(0) + " " + i.getString(1) + " " + i.getString(2))
    }


    //spark DataFrame写入phoenix,需要先建好表
    df.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .options(Map("table" -> "PHOENIXTESTCOPY", "zkUrl" -> "jdbc:phoenix:192.168.187.201:2181"))
      .save()



    // =======================  SparkSQL操作HBase  =======================
    // (测试操作没过,报错)
    /*
       1.jar包需要自己匹配加到maven中
                <!-- https://mvnrepository.com/artifact/com.hortonworks/shc-core -->
                <dependency>
                    <groupId>com.hortonworks</groupId>
                    <artifactId>shc-core</artifactId>
                    <version>1.1.0-2.1-s_2.11</version>
                </dependency>
        2.需要开启enableHiveSupport()支持
     */

    import spark.sql
    // hbase的schema
    val catalog =
      s"""{
         |    "table":{"namespace":"default", "name":"person3"},
         |    "rowkey":"id",
         |    "columns":{
         |    "id":{"cf":"rowkey", "col":"id", "type":"int"},
         |    "name":{"cf":"info", "col":"name", "type":"string"},
         |    "biaoqian":{"cf":"info", "col":"biaoqian", "type":"string"},
         |    "age":{"cf":"info",  "col":"age", "type":"int"}
         |    }
         |    }""".stripMargin

    val dataDF = sc.makeRDD(Seq(("1", "Jack", "M", "26"), ("2", "Rose", "M", "17"))).toDF()
    dataDF.show()
    dataDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .mode("overwrite")
      .save()

    val read = spark.read
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    read.createOrReplaceTempView("table")
    sql("select * from read").show()


    admin.close()
    sc.stop()
  }
}
