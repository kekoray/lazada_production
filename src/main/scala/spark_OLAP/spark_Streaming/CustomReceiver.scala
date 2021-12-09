package spark_OLAP.spark_Streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import kafka.utils.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/*
 * 
 * @ProjectName: lazada_production  
 * @program: spark_OLAP.spark_Streaming
 * @FileName: CustomReceiver 
 * @description:  TODO   
 * @version: 1.0   
 * *
 * @author: koray  
 * @create: 2021-12-08 17:36  
 * @Copyright (c) 2021,All Rights Reserved.
 */

// 自定义接收器
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
