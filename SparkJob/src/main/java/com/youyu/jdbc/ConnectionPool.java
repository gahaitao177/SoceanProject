package com.youyu.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class ConnectionPool
{
  private static LinkedList<Connection> connectionQueue;

  /**
   * 加载驱动
   */
  static
  {
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  /**
   * 获取链接，多线程访问并发控制
   */
  public synchronized static Connection getConnection()
  {
    try
    {
      if (connectionQueue == null)
      {
        connectionQueue = new LinkedList<Connection>();
        for (int i = 0; i < 10; i++)
        {
          Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
          connectionQueue.push(conn);
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    return connectionQueue.poll();
  }

  public static void returnConnection(Connection conn)
  {
    connectionQueue.push(conn);
  }
}
