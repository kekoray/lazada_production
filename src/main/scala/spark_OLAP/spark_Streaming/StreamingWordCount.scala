package spark_OLAP.spark_Streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager}

import kafka.utils.Logging
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.receiver.Receiver


/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.spark_Streaming
 * @FileName: StreamingWordCount 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-11-24 15:27  
 * @Copyright (c) 2021,All Rights Reserved.
 */ object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // 传参有问题报错
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }



    // ***********************************************************************************************************************************************************
    // ***********************************************************************************************************************************************************
    // ********************************************************************  spark-Streaming  ********************************************************************
    // ***********************************************************************************************************************************************************
    // ***********************************************************************************************************************************************************


    // =======================  程序操作  =======================

    // Stream流入口,需设置批处理间隔
    val config = new SparkConf().setAppName("StreamingWordCount").setMaster("local[6]")
    val ssc: StreamingContext = new StreamingContext(config, Seconds(1))

    // 开始接收数据并处理数据
    ssc.start()

    // 等待处理被终止
    ssc.awaitTermination()

    // 手动停止处理
    ssc.stop()

    // 仅停止StreamingContext
    ssc.stop(stopSparkContext = false)



    // =======================  Sources  =======================
    // File Streams
    ssc.textFileStream("/文件路径")
    ssc.fileStream("hdfs://文件路径")

    // Socket Streams
    val line = ssc.socketTextStream(hostname = args(0), port = args(1).toInt, storageLevel = StorageLevel.MEMORY_ONLY)
    val wordCount: DStream[(String, Int)] = line.flatMap(_.split(" ")).map(x => (x, 1))


    // ---------------------  自定义接收器创建输入流  --------------------
    // 1.自定义接收器
    class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) with Logging {

      // 开始接收数据要做的事情
      override def onStart(): Unit = {
        // 启动通过连接接收数据的线程
        new Thread("Socket Receiver") {
          override def run() {
            receive()
          }
        }.start()
      }

      // 停止接收数据要做的事情
      override def onStop(): Unit = {
        print("Socket stop ...")
      }

      // 创建套接字连接并接收数据直到接收器停止
      private def receive() {
        var socket: Socket = null
        var userInput: String = null
        try {
          socket = new Socket(host, port)
          // 直到停止或连接中断继续阅读
          val reader = new BufferedReader(
            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
          userInput = reader.readLine()
          while (!isStopped && userInput != null) {
            store(userInput)
            userInput = reader.readLine()
          }
          reader.close()
          socket.close()

          // 当服务器再次处于活动状态时,重新启动以尝试再次连接
          restart("Trying to connect again")
        } catch {
          case e: java.net.ConnectException =>
            restart("Error connecting to " + host + ":" + port, e)
          case t: Throwable =>
            restart("Error receiving data", t)
        }
      }
    }

    // 2.注册自定义接收器的输入流
    ssc.receiverStream(new CustomReceiver(host = args(0), port = args(1).toInt))


    // ---------------------  kafka输入流  --------------------
    // 1.kafka配置参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 2.topics名称
    val topics = Array("topicA", "topicB")

    // 3.创建kafka输入流
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 4.获取topic数据
    val topicDS: DStream[(String, String)] = stream.map(record => (record.key, record.value))

    // 5.获取偏移量
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }




    // =======================  output-Sink  =======================
    // 1.打印输出
    wordCount.print()

    // 2.保存为序列化对象的序列文件
    wordCount.saveAsObjectFiles("", "csv")

    // 3.保存为文本文件
    wordCount.saveAsTextFiles("/文件路径", "json")

    // 4.保存为Hadoop文件
    wordCount.saveAsHadoopFiles("hdfs://文件路径", "csv")


    /* ---------------------  5.foreachRDD操作,将数据发送到外部系统  --------------------
        对DStream中的RDD,调用foreachPartition,对RDD中每个分区只创建一个连接对象,大大减少创建连接对象的数量;
        但如果partition数量过多,也会导致连接数过多,我们可以使用连接池的方式,使得partition之间可以共享连接对象;
    */
    // A.基于lazy的静态连接,这种方式让一个executor上的task都依赖于同一个连接对象,有可能会造成性能的瓶颈;
    wordCount.foreachRDD(rdd =>
      rdd.foreachPartition(partitionOfRecords => {
        if (partitionOfRecords.size > 0) {
          Class.forName("com.mysql.jdbc.Driver")
          // 1.获取连接对象
          val connection = DriverManager.getConnection("jdbc:mysql://192.168.1.100:3306/test_databases", "root", "123456")
          // 2.处理sql
          partitionOfRecords.foreach(record => connection.createStatement().execute("insert into wordcount(word, num) VALUES('" + record._1 + "'," + record._2 + ")"))
          // 3.关闭连接
          connection.close()
        }
      })
    )


    // B.基于lazy的静态连接池,借助org.apache.commons.pool2框架实现,官方推介的优化方案
    // 1.自定义连接池工厂
    class MysqlConnectionFactory(url: String, userName: String, password: String, className: String) extends BasePooledObjectFactory[Connection] {
      override def create(): Connection = {
        Class.forName(className)
        DriverManager.getConnection(url, userName, password)
      }

      override def wrap(conn: Connection): PooledObject[Connection] = new DefaultPooledObject[Connection](conn)

      override def validateObject(pObj: PooledObject[Connection]) = !pObj.getObject.isClosed

      override def destroyObject(pObj: PooledObject[Connection]) = pObj.getObject.close()
    }

    // 2.自定义连接池
    object ConnectionPool {
      private val pool = new GenericObjectPool[Connection](new MysqlConnectionFactory("jdbc:mysql://192.168.101.88:3306/test_base", "root", "123456", "com.mysql.jdbc.Driver"))

      def getConnection(): Connection = {
        pool.borrowObject()
      }

      def returnConnection(conn: Connection): Unit = {
        pool.returnObject(conn)
      }
    }

    // 3.调用foreachPartition+静态连接池
    wordCount.foreachRDD(rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        // 1.获取连接对象
        lazy val connection = ConnectionPool.getConnection()
        // 2.设为手动提交
        connection.setAutoCommit(false)
        // 3.创建Statement对象,用于将SQL语句发送到数据库
        val stmt = connection.createStatement
        // 4.将SQL添加到命令列表
        partitionOfRecords.foreach(record => stmt.addBatch("insert into wordcount(word, num) VALUES('" + record._1 + "'," + record._2 + ")"))
        // 5.调用executeBatch批量执行SQL命令列表
        stmt.executeBatch()
        // 6.提交事务
        connection.commit()
        // 7.将连接对象返回到连接池中
        ConnectionPool.returnConnection(connection)
      }
    )





    // =======================  转换操作  =======================

    /* ---------------------  updateStateByKey操作  --------------------
        updateStateByKey函数可以进行带历史状态的计算,但是需要chekcpoint保存当前计算的结果;
    */
    // 1.使用updateStateByKey必须设置Checkpoint目录
    ssc.checkpoint("src/main/resources/streaming")

    // 2.编写updateStateByKey的函数体
    def updateFunc(newValue: Seq[Int], runningValue: Option[Int]) = {
      // newValue是当前批次Key的所有Value的序列集合
      val currentBatchSum: Int = newValue.sum
      // runningValue是key对应的以前的值
      val state: Int = runningValue.getOrElse(0)
      // 返回key的最新值,会再次进入Checkpoint中当作状态存储
      Some(currentBatchSum + state)
    }

    // 3.初始化状态值,可以将上一次程序结束的结果状态保存到MySQL中,等下次执行时读取状态值并继续累加即可,这算是updateStateByKey的优化
    val sc: SparkContext = SparkContext.getOrCreate(config)
    val initialRDD: RDD[(String, Int)] = sc.parallelize(Array(("hadoop", 100), ("spark", 25)))

    // 4.调用updateStateByKey
    val wordCounts: DStream[(String, Int)] = wordCount.updateStateByKey(updateFunc(_, _), new HashPartitioner(ssc.sparkContext.defaultParallelism), initialRDD)



    /* ---------------------  window操作  --------------------
        windowDuration窗口长度:窗口长度配置的就是将多长时间内的RDD合并为一个
        slideDuration滑动间隔:每隔多久生成一个window

        滑动时间的问题:
            1.如果windowDuration > slideDuration,则在每一个不同的窗口中,可能计算了重复的数据
            2.如果windowDuration < slideDuration,则在每一个不同的窗口之间,有一些数据未能计算进去
     */
    // 通过window操作,会将流分为多个窗口,再求聚合
    val wordsWindow: DStream[(String, Int)] = wordCount.window(Seconds(30), Seconds(10)).reduceByKey((_ + _))

    // 同上操作的优化方案 ==> 开启窗口并进行reduceByKey聚合,需要写对应的参数名 (reduceFunc,windowDuration窗口长度,slideDuration滑动间隔)
    val wordCounts2: DStream[(String, Int)] = wordCount.reduceByKeyAndWindow(reduceFunc = (_ + _), windowDuration = Seconds(5), slideDuration = Seconds(5))


  }

}
