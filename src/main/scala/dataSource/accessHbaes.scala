package dataSource

import java.util.UUID

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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

    val spark = SparkSession.builder().appName("accessHbaes").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    // hbase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "192.168.100.216")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.defaults.for.version.skip", "true")

    conf.set(TableInputFormat.INPUT_TABLE, "person")


    val hbaseConn = ConnectionFactory.createConnection(conf)
    val admin = hbaseConn.getAdmin
    println(admin.listTableNames().toList)


    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()
    println(hBaseRDD.count())


    println(hBaseRDD.map(_._2).count())

    import spark.implicits._


    hBaseRDD.map({ case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue("name".getBytes, "china".getBytes))
      (key, name)
    }).toDF("id", "name").show(false)


    // =======================  通过HTable中put方法  =======================
    // =======================  通过TableOutputFormat向HBase写数据  =======================
    // =======================  通过bulkload向HBase写数据  =======================


    /*使用 saveAsHadoopDataset 写入数据
      使用 newAPIHadoopRDD 读取数据
      Spark DataFrame 通过 Phoenix 读写 HBase*/
    sc.stop()


  }
}
