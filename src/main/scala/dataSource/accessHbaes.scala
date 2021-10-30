package dataSource

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder().appName("accessHbaes").master("local[*]").getOrCreate()
      val sc: SparkContext = spark.sparkContext

      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum","cdh1")
      conf.set("hbase.zookeeper.property.clientPort","2181")
      conf.set(TableInputFormat.INPUT_TABLE,"default")

      val hBaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result]).cache()

      // RDD数据操作
      val data = hBaseRDD.map(x => {
        val result = x._2
        val key = Bytes.toString(result.getRow)
        val value = Bytes.toString(result.getValue("info".getBytes,"click_count".getBytes))
        (key,value)
      })

      data.foreach(println)

      sc.stop()
    }
  }
}
