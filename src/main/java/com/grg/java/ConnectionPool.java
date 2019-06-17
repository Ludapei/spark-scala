package com.grg.java;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.LinkedList;


// 连接池基本的思想是在系统初始化的时候，将数据库连接作为对象存储在内存中，当用户需要访问数据库时，并非建立一个新的连接，
// 而是从连接池中取出一个已建立的空闲连接对象。使用完毕后，用户也并非将连接关闭，而是将连接放回连接池中，以供下一个请求访问使用。
// https://blog.csdn.net/fuyuwei2015/article/details/72419975
public class ConnectionPool {
//    private static LinkedList<Connection> connectionQueue;
////  静态代码块先被执行，且被执行一次
//    static {
//        try {
//            Class.forName("com.mysql.jdbc.Driver");
//        }catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public synchronized static Connection getConnection() {
//        try {
//            if (connectionQueue == null) {
//                connectionQueue = new LinkedList<Connection>();
//                for (int i = 0;i < 5;i ++) {
//                    Connection conn = DriverManager.getConnection(
//                            "jdbc:mysql://127.0.0.1:3306/ldp?useUnicode=true&characterEncoding=utf8",
//                            "root",
//                            "123456"
//                    );
//                    connectionQueue.push(conn);
//                }
//            }
//        }catch (Exception e) {
//            e.printStackTrace();
//        }
//        return connectionQueue.poll();
//    }
//
//    public static void returnConnection(Connection conn) {
//        connectionQueue.push(conn);
//    }

    private static LinkedList<Connection> connectionQueue;
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
    
    public synchronized static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                for (int i = 0; i < 5; i++) {
                    Connection connection=DriverManager.getConnection(
                            "jdbc:mysql://127.0.0.1:3306/ldp?",
                            "root",
                            "123456"
                    );
                    connectionQueue.push(connection);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return  connectionQueue.poll();
    }

    public static void returnConnection(Connection connection) {
        connectionQueue.push(connection);
    }
}