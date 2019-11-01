package com.alibaba.datax.plugin.reader.hivereader;

/**
 * @author daixinxuan
 * @date 2019/10/27 14:17
 */
public class Constant {
    static final int TIMEOUT_SECONDS = 15;
    static final int MAX_TRY_TIMES = 4;
    static final int SOCKET_TIMEOUT_INSECOND = 172800;

    public static final String TMP_PREFIX = "/user/datax_tmp/";
    public static final String TABLE_NAME_PREFIX = "hive_reader_";

    public static final String SOURCE_FILES = "sourceFiles";
    public static final String IS_TABLE_MODE = "isTableMode";

    public static final String TEXT = "TEXT";
    public static final String ORC = "ORC";
    public static final String CSV = "CSV";
    public static final String SEQ = "SEQ";
    public static final String RC = "RC";
}
