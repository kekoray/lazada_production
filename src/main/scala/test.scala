import org.apache.hadoop.log.LogLevel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * 
 * @ProjectName: lazada_production  
 * @program:    
 * @FileName: test 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-10-11 17:01  
 * @Copyright (c) 2021,All Rights Reserved.
 */
object test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[8]").setAppName("apptest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")
    val textrdd: RDD[String] = sc.textFile("src/main/resources/wordcount.txt")
    val result = textrdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.foreach(print)

//    result.cache()
//    sc.setCheckpointDir("src/main/resources")





    sc.stop()


  }


}
