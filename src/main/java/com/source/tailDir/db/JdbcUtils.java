package com.source.tailDir.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Created by ibm on 2015/10/22.
 */
public class JdbcUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtils.class);

    /**
     * 数据库配置
     */
    private static final String driver;
    private static final String url;
    private static final String username;
    private static final String password;
    private static final String dbPrefix;

    /**
     * 定义一个用于放置数据库连接的局部线程变量（使每个线程都拥有自己的连接）
     */
    private static ThreadLocal<Connection> connContainer = new ThreadLocal<Connection>();

    static {
        try {
            InputStream in = JdbcUtils.class.getClassLoader().getResourceAsStream("dbPools.properties");
            Properties props = new Properties();
            props.load(in);
            driver = props.getProperty("drivers");
            url = props.getProperty("url");
            username = props.getProperty("username");
            password = props.getProperty("password");
            dbPrefix = props.getProperty("dbPrefix");
            LOGGER.info("connection info: {}, {}, {}, {}, {}", driver, url, username, password, dbPrefix);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String getDbPrefix() {
        return dbPrefix;
    }

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection conn = connContainer.get();
        try {
            if (conn == null) {
                Class.forName(driver);
                conn = DriverManager.getConnection(url, username, password);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connContainer.set(conn);
        }
        return conn;
    }

    /**
     * 关闭连接
     */
    public static void closeConnection() {
        Connection conn = connContainer.get();
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connContainer.remove();
        }
    }

}
