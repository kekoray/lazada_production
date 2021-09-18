import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 *
 * @ProjectName: lazada_production
 * @program:
 * @FileName: WordCounts
 * @description: TODO
 * @version: 1.0
 *           *
 * @author: koray
 * @create: 2021-09-07 14:45
 * @Copyright (c) 2021,All Rights Reserved.
 */

object WordCounts {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val source: RDD[String] = sc.textFile("src/main/resources/wordcount.txt")
    val words: RDD[String] = source.flatMap(_.split(" "))
    val wordsTuple: RDD[(String, Int)] = words.map((_, 1))
    val result: RDD[(String, Int)] = wordsTuple.reduceByKey((x, y) => x + y)
    result.foreach(println(_))
    sc.stop()

  }
}
