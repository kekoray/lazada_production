package spark_Streaming

import java.sql.Connection

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}

object ConnectionPool {
  private val pool = new GenericObjectPool[Connection](new MysqlConnectionFactory("jdbc:mysql://192.168.101.88:3306/test_base", "root", "123456", "com.mysql.jdbc.Driver"))

  def getConnection(): Connection = {
    pool.borrowObject()
  }

  def returnConnection(conn: Connection): Unit = {
    pool.returnObject(conn)
  }
}