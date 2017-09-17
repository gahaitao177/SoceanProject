package com.youyu.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}

import scala.collection.mutable

/**
  * Created by xiaxc on 2017/5/11.
  */
object HbaseUtilDemo extends Serializable {
  private val conf = HBaseConfiguration.create()
  /*private val port = "2181"
  private val quorum = "slave1,slave2,slave3"
  conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
  conf.set(HConstants.ZOOKEEPER_QUORUM, quorum) // hosts*/
  private val connection = ConnectionFactory.createConnection(conf)

  def getHbaseConn: Connection = connection

  /**
    * 获取表中所有的rowkey
    *
    * @param tableName
    * @return
    */
  def getAllRowKey(tableName: String): mutable.HashMap[String, Integer] = {
    import scala.collection.mutable.HashMap

    val table = connection.getTable(TableName.valueOf(tableName))
    val scan = new Scan
    val map: HashMap[String, Integer] = HashMap()
    val results = table.getScanner(scan)

    import scala.collection.JavaConversions._
    for (result <- results) {
      for (cell <- result.rawCells) {
        val key = new String(CellUtil.cloneRow(cell))
        map.put(key, 1)
      }
    }

    map
  }

  /**
    * 插入数据前先判断当前插入的rowkey在表中是否存在
    *
    * @param tableName
    * @param rowKey
    * @return
    */
  def isExistRowKey(tableName: String, rowKey: String): Boolean = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(rowKey.getBytes)
    val r: Result = table.get(get)

    val flag: Boolean = r.isEmpty

    flag
  }
}