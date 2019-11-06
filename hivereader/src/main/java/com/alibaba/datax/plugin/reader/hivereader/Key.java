package com.alibaba.datax.plugin.reader.hivereader;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */

    /**
     * hive自定义SQL语句，运用于部分表数据同步。
     * 必传属性,数组类型。
     */
    public final static String HIVE_SQL = "sqls";
    public final static String JDBC_URL = "jdbcUrl";
    public final static String USER = "user";
    public final static String PASSWORD = "password";

    /**
     * hdfs地址，必传参数
     */
    public final static String DEFAULT_FS = "defaultFS";

    /**
     * kerberos auth相关参数
     */
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

}
