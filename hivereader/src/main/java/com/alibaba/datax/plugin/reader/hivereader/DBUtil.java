package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * @author daixinxuan
 * @date 2019/10/29 10:54
 */
public class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    private DBUtil() {}

    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, Constant.SOCKET_TIMEOUT_INSECOND);
    }

    private static ResultSet query(Connection conn, String sql, int queryTimeout)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        return query(stmt, sql);
    }

    public static ResultSet query(Statement stmt, String sql)
            throws SQLException {
        return stmt.executeQuery(sql);
    }

    public static Connection getConnection(final String url, final String user, final String pass) {
        try {
            final String socketTimeout = String.valueOf(Constant.SOCKET_TIMEOUT_INSECOND * 1000);
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                @Override
                public Connection call() throws Exception {
                    return DBUtil.connect(url, user,
                            pass, socketTimeout);
                }
            }, 9, 1000L, true);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    HiveReaderErrorCode.CONN_DB_ERROR,
                    String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", url), e);
        }

    }

    private static synchronized Connection connect(String url, String user, String pass, String socketTimeout) {
        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        return connect(url, prop);
    }

    private static Connection connect(String url, Properties prop) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            DriverManager.setLoginTimeout(Constant.TIMEOUT_SECONDS);
            return DriverManager.getConnection(url, prop);
        } catch (Exception e) {
            throw DataXException.asDataXException(HiveReaderErrorCode.CONN_DB_ERROR," 具体错误信息为："+e);
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException unused) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException unused) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException unused) {
            }
        }
    }

}
