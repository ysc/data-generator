package org.apdplat.data.generator.mysql;


import org.apdplat.data.generator.utils.Config;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Created by ysc on 18/04/2018.
 */
public class MySQLUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLUtils.class);

    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = Config.getStringValue("mysql.url") == null ? "jdbc:mysql://192.168.252.193:3306/demo?useUnicode=true&characterEncoding=utf8" : Config.getStringValue("mysql.url");
    private static final String USER = Config.getStringValue("mysql.user") == null ? "root" : Config.getStringValue("mysql.user");
    private static final String PASSWORD = Config.getStringValue("mysql.password") == null ? "root" : Config.getStringValue("mysql.password");

    private static DataSource dataSource = null;

    static {
        try {
            Class.forName(DRIVER);
            dataSource = setupDataSource(URL, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            LOGGER.error("MySQL驱动加载失败：", e);
        }
    }

    private MySQLUtils() {
    }

    public static int getCount(String table){
        Connection con = MySQLUtils.getConnection();
        if(con == null){
            return 0;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try{
            pst = con.prepareStatement("select count(1) from "+table);
            rs = pst.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
        } catch (Exception e) {
            LOGGER.error("查询失败", e);
        } finally {
            MySQLUtils.close(con, pst, rs);
        }
        return 0;
    }

    public static void clean(String table){
        Connection con = getConnection();
        if(con == null){
            return ;
        }
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = con.prepareStatement("delete from "+table);
            pst.execute();
            LOGGER.info("清理数据成功, table: "+table);
        } catch (Exception e) {
            LOGGER.error("清理数据失败, table: "+table, e);
        } finally {
            close(con, pst, rs);
        }
    }

    public static Connection getConnection() {
        Connection con = null;
        try {
            con = dataSource.getConnection();
        } catch (Exception e) {
            LOGGER.error("MySQL获取数据库连接失败：", e);
        }
        return con;
    }

    private static DataSource setupDataSource(String connectUri, String uname, String passwd) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory =
                new DriverManagerConnectionFactory(connectUri, uname, passwd);

        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public static void close(Statement st) {
        close(null, st, null);
    }

    public static void close(Statement st, ResultSet rs) {
        close(null, st, rs);
    }

    public static void close(Connection con, Statement st, ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }
            if (st != null) {
                st.close();
                st = null;
            }
            if (con != null) {
                con.close();
                con = null;
            }
        } catch (SQLException e) {
            LOGGER.error("数据库关闭失败", e);
        }
    }

    public static void close(Connection con, Statement st) {
        close(con, st, null);
    }

    public static void close(Connection con) {
        close(con, null, null);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getConnection());
    }
}