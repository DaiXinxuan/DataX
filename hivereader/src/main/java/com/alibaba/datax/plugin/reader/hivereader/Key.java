package com.alibaba.datax.plugin.reader.hivereader;

public final class Key {

    /**
     * 此处声明插件用到的需要插件使用者提供的配置项
     */

    /**
     * hive自定义SQL语句，运用于部分表数据同步。
     * 非必传属性,数组类型。
     * 当该属性不为空时，tables属性失效.
     */
    public final static String HIVE_SQL = "sqls";
    public final static String JDBC_URL = "jdbcUrl";
    public final static String USER = "user";
    public final static String PASSWORD = "password";


    /**
     * 以下三参数运用于全表数据同步场景。
     * 非必传属性。但不传sqls属性或sqls对应字段为空时，该三字段必传
     * tables支持多表,数组类型。
     * hiveRoot为hive的根目录
     * fileType指定文件类型
     */
    public final static String TABLE = "tables";
    public final static String HIVE_ROOT = "hiveRoot";
    public static final String FILE_TYPE = "fileType";

    /**
     * hdfs地址，必传参数
     */
    public final static String DEFAULT_FS = "defaultFS";

    public static final String HADOOP_CONFIG = "hadoopConfig";
    public static final String HAVE_KERBEROS = "haveKerberos";
    public static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    public static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";
}
