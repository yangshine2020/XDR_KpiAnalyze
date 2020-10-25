package cn.com.dtmobile.util;

import java.sql.*;
import java.util.HashMap;

/**
 * 数据库连接类
 * 说明:封装了 无参，有参，存储过程的调用
 */
public class DBUtil {

    private static final String DRIVER = "oracle.jdbc.driver.OracleDriver";
    private String URLSTR;
    private static final String USERNAME = "scott";
    private static final String USERPASSWORD = "tiger";
    private Connection connnection = null;
    private PreparedStatement preparedStatement = null;
    private CallableStatement callableStatement = null;
    private ResultSet resultSet = null;


    public DBUtil(String url) {
        this.URLSTR = url;
    }

    static {
        try {
            // 加载数据库驱动程序
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("加载驱动错误");
            System.out.println(e.getMessage());
        }
    }

    /**
     * 建立数据库连接
     *
     * @return 数据库连接
     */
    public Connection getConnection() {
        try {
            // 获取连接
            connnection = DriverManager.getConnection(URLSTR, USERNAME,
                    USERPASSWORD);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return connnection;
    }

    /**
     * insert update delete SQL语句的执行的统一方法
     *
     * @param sql    SQL语句
     * @param params 参数数组，若没有参数则为null
     * @return 受影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        // 受影响的行数
        int affectedLine = 0;

        try {
            // 获得连接
            connnection = this.getConnection();
            // 调用SQL
            preparedStatement = connnection.prepareStatement(sql);

            // 参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }

            // 执行
            affectedLine = preparedStatement.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            // 释放资源
            closeAll();
        }
        return affectedLine;
    }

    /**
     * SQL 查询将查询结果直接放入ResultSet中
     *
     * @param sql    SQL语句
     * @param params 参数数组，若没有参数则为null
     * @return 结果集
     */
    private ResultSet executeQueryRS(String sql, Object[] params) {
        try {
            // 获得连接
            connnection = this.getConnection();

            // 调用SQL
            preparedStatement = connnection.prepareStatement(sql);

            // 参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    preparedStatement.setObject(i + 1, params[i]);
                }
            }

            // 执行
            resultSet = preparedStatement.executeQuery();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return resultSet;
    }

    /**
     * 获取结果集，并将结果放在List中
     *
     * @param sql SQL语句
     * @return List
     * 结果集
     */
    public HashMap<String, String> excuteQuery(String sql, String[] params) {
        // 执行SQL获得结果集
        ResultSet rs = executeQueryRS(sql, params);

        // 创建ResultSetMetaData对象
        ResultSetMetaData rsmd = null;

        // 结果集列数
        int columnCount = 0;
        try {
            rsmd = rs.getMetaData();

            // 获得结果集列数
            columnCount = rsmd.getColumnCount();
        } catch (SQLException e1) {
            System.out.println(e1.getMessage());
        }

        // 创建List
        HashMap<String, String> map = new HashMap<String, String>();

        try {
            // 将ResultSet的结果保存到List中
            while (rs.next()) {

                for (int i = 1; i <= columnCount; i++) {
                    map.put(rsmd.getColumnLabel(i), rs.getString(i));
                }

            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            // 关闭所有资源
            closeAll();
        }

        return map;
    }

    /**
     * 存储过程带有一个输出参数的方法
     *
     * @param sql         存储过程语句
     * @param params      参数数组
     * @param outParamPos 输出参数位置
     * @param SqlType     输出参数类型
     * @return 输出参数的值
     */
    public Object excuteQuery(String sql, Object[] params, int outParamPos, int SqlType) {
        Object object = null;
        connnection = this.getConnection();
        try {
            // 调用存储过程
            callableStatement = connnection.prepareCall(sql);

            // 给参数赋值
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    callableStatement.setObject(i + 1, params[i]);
                }
            }

            // 注册输出参数
            callableStatement.registerOutParameter(outParamPos, SqlType);

            // 执行
            callableStatement.execute();

            // 得到输出参数
            object = callableStatement.getObject(outParamPos);

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        } finally {
            // 释放资源
            closeAll();
        }

        return object;
    }

    /**
     * 关闭所有资源
     */
    private void closeAll() {
        // 关闭结果集对象
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        // 关闭PreparedStatement对象
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        // 关闭CallableStatement 对象
        if (callableStatement != null) {
            try {
                callableStatement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        // 关闭Connection 对象
        if (connnection != null) {
            try {
                connnection.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }
    }

    public HashMap<String, Integer> select() {

        connnection = this.getConnection();

        HashMap<String, Integer> result = new HashMap<String, Integer>();

        Statement sm = null;
        try {
            sm = connnection.createStatement();
            ResultSet rs = sm.executeQuery("select field,value from data_threshold");

            while (rs.next()) {
                int value = 0;
                if (rs.getString(2) != null) {
                    value = Integer.parseInt(rs.getString(2));
                }
                result.put(rs.getString(1), value);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            closeAll();
        }
        return result;
    }


    public String select(String table, String where) {
        connnection = this.getConnection();
        String result = "";
        Statement sm = null;
        try {
            sm = connnection.createStatement();
            String sql = "";
            sql = "select operator,value from " + table + " where field=" + where;
            ResultSet rs = sm.executeQuery(sql);
            while (rs.next()) {
                result += rs.getString(1) + rs.getString(2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            closeAll();
        }
        return result;
    }


    public static void main(String ags[]) throws SQLException {

        DBUtil db = new DBUtil("jdbc:oracle://172.30.4.159:1521/morpho0712");

//        HashMap<String,Integer> r = db.select() ;
//
//        System.out.println(r.get("tiemdelay"));

        String[] p = new String[2];
        p[0] = "1";
        p[1] = "2";
        db.executeQueryRS("select * from  volte_gt_user_ana_base60", p);


    }


}
