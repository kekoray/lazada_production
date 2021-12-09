
import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, ConnectionFactory, Put, Result, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
//import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

import scala.util.Try


/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.dataSource
 * @FileName: accessHbaes 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-18 15:12  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object accessHbaes {
  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("accessHbaes").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    // hbase的配置
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "192.168.100.216")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    //在IDE中设置此项为true，避免出现"hbase-default.xml"版本不匹配的运行时异常
    hbaseConf.set("hbase.defaults.for.version.skip", "true")

    // 获取admina管理员
    val hbaseConn = ConnectionFactory.createConnection(hbaseConf)
    val admin = hbaseConn.getAdmin
    print(admin.listTableNames().toList)

    // 删除表
    //    admin.deleteTable(TableName.valueOf("table_name"))


    /*
    TableInputFormat/TableOutputFormat是org.apache.hadoop.hbase.mapreduce包下
     */
    // =======================  读取数据  =======================

    // 设置查询的表名
    //    hbaseConf.set(TableInputFormat.INPUT_TABLE, "person")
    //    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(hbaseConf,
    //      classOf[TableInputFormat],
    //      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    //      classOf[org.apache.hadoop.hbase.client.Result]).cache()
    //
    //    // 遍历结果
    //    hBaseRDD.map({ case (_, result) =>
    //      // 获取行键
    //      val key = Bytes.toString(result.getRow)
    //      // 通过列族和列名获取值
    //      val name = Bytes.toString(result.getValue("name".getBytes, "china".getBytes))
    //      (key, name)
    //    }).toDF("id", "name").show(false)
    //
    //

    //
    //    // =======================  写取数据  =======================
    //
    //    // 判断表是否存在,不存在则创建表
    //    if (!admin.tableExists(TableName.valueOf("person2"))) {
    //      val table_desc = TableDescriptorBuilder.newBuilder(TableName.valueOf("person2"))
    //      //指定列簇,不需要创建列
    //      val col_desc = ColumnFamilyDescriptorBuilder.newBuilder("info".getBytes()).build()
    //      table_desc.setColumnFamily(col_desc)
    //      admin.createTable(table_desc.build())
    //    }
    //
    //    // 设置写入的表名
    //    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "person2")
    //    val job = Job.getInstance(hbaseConf)
    //    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    //    job.setOutputValueClass(classOf[Result])
    //    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    //
    //
    //    val data = sc.makeRDD(Array("1,Jack,M,26", "2,Rose,M,17")).map(_.split(","))
    //    val dataRDD = data.map(arr => {
    //      // 设置行健
    //      val put = new Put(Bytes.toBytes(arr(0)))
    //      // 设置列族和列名和值
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2)))
    //      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3).toInt))
    //      (new ImmutableBytesWritable, put)
    //    })
    //
    //    // 数据写入hbase
    //    dataRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)


    // =======================  SparkSQL操作HBase  =======================
    /*
    jar包自己匹配加到maven中
    <!-- https://mvnrepository.com/artifact/com.hortonworks/shc-core -->
    <dependency>
        <groupId>com.hortonworks</groupId>
        <artifactId>shc-core</artifactId>
        <version>1.1.0-2.1-s_2.11</version>
    </dependency>

     */
    import spark.sql
    // rowkey必须定义为列(col0),该列具有特定的cf(rowkey)
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

    //    val dataDF = sc.makeRDD(Seq(("1", "Jack", "M", "26"), ("2", "Rose", "M", "17"))).toDF()
    //    dataDF.show()
    //    dataDF.write.options(
    //      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4"))
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .mode("overwrite")
    //      .save()

    //    val read = spark.read
    //      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .load()
    //    read.createOrReplaceTempView("table")
    //    sql("select * from read").show()


    sc.stop()


  }
}