package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.datax.plugin.writer.hivereader.DFSUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author daixinxuan
 * @date 2019/10/26 17:17
 */
public class HiveReader extends Reader {

    /**
     * init -> prepare -> split
     */
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration readerOriginConfig;
        /**
         * 是否是全量同步，全量同步和使用SQL语句的部分同步实现方式不同
         */
        private boolean isTableMode;

        private List<String> sqls;
        private String hiveRoot;
        private List<String> paths;
        private HashSet<String> sourceFiles;
        private String specifiedFileType;

        private com.alibaba.datax.plugin.writer.hivereader.DFSUtil dfsUtil;

        @Override
        public void init() {
            LOG.info("init() begin...");
            this.readerOriginConfig = super.getPluginJobConf();
            this.validate();
            dfsUtil = new DFSUtil(readerOriginConfig);
            LOG.info("init() ok and end...");
        }

        public void validate() {
            readerOriginConfig.getNecessaryValue(Key.DEFAULT_FS,
                    HiveReaderErrorCode.DEFAULT_FS_NOT_FIND_ERROR);
            sqls = readerOriginConfig.getList(Key.HIVE_SQL, String.class);

            if (sqls == null || sqls.size() == 0) {
                List<String> tables;
                hiveRoot = readerOriginConfig.getNecessaryValue(Key.HIVE_ROOT, HiveReaderErrorCode.REQUIRED_VALUE);
                String tableStr = readerOriginConfig.getNecessaryValue(Key.TABLE, HiveReaderErrorCode.REQUIRED_VALUE);
                specifiedFileType = readerOriginConfig.getNecessaryValue(Key.FILE_TYPE, HiveReaderErrorCode.REQUIRED_VALUE);

                if (!tableStr.startsWith("[") && !tableStr.endsWith("]")) {
                    tables = new ArrayList<String>();
                    tables.add(tableStr);
                } else {
                    tables = readerOriginConfig.getList(Key.TABLE, String.class);
                }
                // 校验hive根目录
                if (!hiveRoot.startsWith("/")) {
                    String message = String.format("请检查参数hiveRoot:[%s],需要配置为绝对路径", hiveRoot);
                    LOG.error(message);
                    throw DataXException.asDataXException(HiveReaderErrorCode.ILLEGAL_VALUE, message);
                }
                if (!hiveRoot.endsWith("/")) {
                    hiveRoot += "/";
                }
                // 校验文件类型
                if( !specifiedFileType.equalsIgnoreCase(Constant.ORC) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.TEXT) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.CSV) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.SEQ) &&
                        !specifiedFileType.equalsIgnoreCase(Constant.RC)){
                    String message = "HiveReader插件目前支持ORC, TEXT, CSV, SEQUENCE, RC五种格式的文件," +
                            "请将fileType选项的值配置为ORC, TEXT, CSV, SEQUENCE 或者 RC";
                    throw DataXException.asDataXException(HiveReaderErrorCode.FILE_TYPE_ERROR, message);
                }
                if(this.specifiedFileType.equalsIgnoreCase(Constant.CSV)){
                    //compress校验
                    UnstructuredStorageReaderUtil.validateCompress(this.readerOriginConfig);
                    UnstructuredStorageReaderUtil.validateCsvReaderConfig(this.readerOriginConfig);
                }

                // 全量同步模式
                isTableMode = true;
                paths = new ArrayList<String>();
                for (String table : tables) {
                    paths.add(hiveRoot + table + "/*");
                }
            } else {
                readerOriginConfig.getNecessaryValue(Key.JDBC_URL, HiveReaderErrorCode.REQUIRED_VALUE);
                readerOriginConfig.getNecessaryValue(Key.USER, HiveReaderErrorCode.REQUIRED_VALUE);
                readerOriginConfig.getNecessaryValue(Key.PASSWORD, HiveReaderErrorCode.REQUIRED_VALUE);
                // 部分同步模式
                isTableMode = false;
            }
        }

        @Override
        public void prepare() {
            if (isTableMode) {
                LOG.info("prepare(), start to getAllFiles...");
                this.sourceFiles = dfsUtil.getAllFiles(paths, specifiedFileType);
                LOG.info(String.format("您即将读取的文件数为: [%s], 列表为: [%s]",
                        this.sourceFiles.size(),
                        StringUtils.join(this.sourceFiles, ",")));
            } else {
                LOG.info(String.format("您即将执行的SQL条数为: [%s], 列表为: [%s]",
                        this.sqls.size(),
                        StringUtils.join(this.sqls, ";")));
            }
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.info("split() begin...");
            List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
            if (isTableMode) {
                // 全量同步通过读文件形式实现
                int splitNumber = this.sourceFiles.size();
                if (0 == splitNumber) {
                    throw DataXException.asDataXException(HiveReaderErrorCode.NO_FILE_FOUND,
                            String.format("未能找到待读取的文件,请确认您的配置项hiveRoot: %s及tables: %s",
                                    this.readerOriginConfig.getString(Key.HIVE_ROOT),
                                    readerOriginConfig.getList(Key.TABLE)));
                }
                List<List<String>> splitedSourceFiles = this.splitSourceFiles(new ArrayList<String>(this.sourceFiles), adviceNumber);
                for (List<String> files : splitedSourceFiles) {
                    Configuration splitedConfig = this.readerOriginConfig.clone();
                    splitedConfig.set(Constant.SOURCE_FILES, files);
                    splitedConfig.set(Constant.IS_TABLE_MODE, true);
                    readerSplitConfigs.add(splitedConfig);
                }
            } else {
                // 自定义SQL部分同步通过切分sql条数实现split
                List<List<String>> splitedSqls = this.splitSourceFiles(sqls, adviceNumber);
                for (List<String> sqls : splitedSqls) {
                    Configuration splitedConfig = this.readerOriginConfig.clone();
                    splitedConfig.set(Key.HIVE_SQL, sqls);
                    splitedConfig.set(Constant.IS_TABLE_MODE, false);
                    readerSplitConfigs.add(splitedConfig);
                }
            }
            return readerSplitConfigs;
        }

        private <T> List<List<T>> splitSourceFiles(final List<T> sourceList, int adviceNumber) {
            List<List<T>> splitedList = new ArrayList<List<T>>();
            int averageLength = sourceList.size() / adviceNumber;
            averageLength = averageLength == 0 ? 1 : averageLength;

            for (int begin = 0, end = 0; begin < sourceList.size(); begin = end) {
                end = begin + averageLength;
                if (end > sourceList.size()) {
                    end = sourceList.size();
                }
                splitedList.add(sourceList.subList(begin, end));
            }
            return splitedList;
        }

        @Override
        public void destroy() {

        }

    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private static final boolean IS_DEBUG = LOG.isDebugEnabled();
        protected final byte[] EMPTY_CHAR_ARRAY = new byte[0];

        private Configuration taskConfig;
        private boolean isTableMode;
        private DFSUtil dfsUtil;
        private String basicMsg;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.isTableMode = taskConfig.getBool(Constant.IS_TABLE_MODE);
            this.dfsUtil = new DFSUtil(taskConfig);
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("read start");
            if (isTableMode) {
                List<String> sourceFiles = taskConfig.getList(Constant.SOURCE_FILES, String.class);
                String specifiedFileType = taskConfig.getString(Key.FILE_TYPE);
                for (String sourceFile : sourceFiles) {
                    LOG.info(String.format("reading file : [%s]", sourceFile));

                    if(specifiedFileType.equalsIgnoreCase(Constant.TEXT)
                            || specifiedFileType.equalsIgnoreCase(Constant.CSV)) {
                        InputStream inputStream = dfsUtil.getInputStream(sourceFile);
                        UnstructuredStorageReaderUtil.readFromStream(inputStream, sourceFile, this.taskConfig,
                                recordSender, this.getTaskPluginCollector());
                    }else if(specifiedFileType.equalsIgnoreCase(Constant.ORC)){

                        dfsUtil.orcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    }else if(specifiedFileType.equalsIgnoreCase(Constant.SEQ)){

                        dfsUtil.sequenceFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    }else if(specifiedFileType.equalsIgnoreCase(Constant.RC)){

                        dfsUtil.rcFileStartRead(sourceFile, this.taskConfig, recordSender, this.getTaskPluginCollector());
                    }else {
                        String message = "HiveReader插件目前支持ORC, TEXT, CSV, SEQUENCE, RC五种格式的文件," +
                                "请将fileType选项的值配置为ORC, TEXT, CSV, SEQUENCE 或者 RC";
                        throw DataXException.asDataXException(HiveReaderErrorCode.FILE_TYPE_UNSUPPORT, message);
                    }

                    if(recordSender != null){
                        recordSender.flush();
                    }
                }

                LOG.info("end read source files...");
            } else {
                List<String> querySqls = taskConfig.getList(Key.HIVE_SQL, String.class);
                String user = taskConfig.getString(Key.USER);
                String pass = taskConfig.getString(Key.PASSWORD);
                String jdbcUrl = taskConfig.getString(Key.JDBC_URL);

                basicMsg = String.format("jdbcUrl:[%s], taskId:[%s]", jdbcUrl, getTaskId());
                PerfTrace.getInstance().addTaskDetails(getTaskId(), basicMsg);

                Connection connection = DBUtil.getConnection(jdbcUrl, user, pass);
                ResultSet rs = null;
                try {
                    for (String sql : querySqls) {
                        LOG.info("Begin to read record by Sql: [{}\n] {}.",
                                sql, basicMsg);
                        PerfRecord queryPerfRecord = new PerfRecord(getTaskGroupId(), getTaskId(), PerfRecord.PHASE.SQL_QUERY);
                        queryPerfRecord.start();

                        long rsNextUsedTime = 0;
                        long lastTime = System.nanoTime();
                        rs = DBUtil.query(connection, sql);
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnNumber = metaData.getColumnCount();
                        while (rs.next()) {
                            rsNextUsedTime += (System.nanoTime() - lastTime);
                            transportOneRecord(recordSender, rs, metaData,
                                    columnNumber, "", getTaskPluginCollector());
                            lastTime = System.nanoTime();
                        }
                        queryPerfRecord.end(rsNextUsedTime);
                        LOG.info("Finished read record by Sql: [{}\n] {}.",
                                sql, basicMsg);
                    }
                } catch (Exception e) {
                    throw  DataXException.asDataXException(HiveReaderErrorCode.SQL_EXECUTE_FAIL,
                            "执行的SQL为: " + querySqls + " 具体错误信息为：" + e);
                } finally {
                    DBUtil.closeDBResources(rs, null, connection);
                }
            }
        }

        @Override
        public void destroy() {

        }

        protected Record transportOneRecord(RecordSender recordSender, ResultSet rs,
                                            ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                            TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender,rs,metaData,columnNumber,mandatoryEncoding,taskPluginCollector);
            recordSender.sendToWriter(record);
            return record;
        }

        protected Record buildRecord(RecordSender recordSender,ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
                                     TaskPluginCollector taskPluginCollector) {
            Record record = recordSender.createRecord();

            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                        case Types.CHAR:
                        case Types.NCHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            String rawData;
                            if(StringUtils.isBlank(mandatoryEncoding)){
                                rawData = rs.getString(i);
                            }else{
                                rawData = new String((rs.getBytes(i) == null ? EMPTY_CHAR_ARRAY :
                                        rs.getBytes(i)), mandatoryEncoding);
                            }
                            record.addColumn(new StringColumn(rawData));
                            break;

                        case Types.CLOB:
                        case Types.NCLOB:
                            record.addColumn(new StringColumn(rs.getString(i)));
                            break;

                        case Types.SMALLINT:
                        case Types.TINYINT:
                        case Types.INTEGER:
                        case Types.BIGINT:
                            record.addColumn(new LongColumn(rs.getString(i)));
                            break;

                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.FLOAT:
                        case Types.REAL:
                        case Types.DOUBLE:
                            record.addColumn(new DoubleColumn(rs.getString(i)));
                            break;

                        case Types.TIME:
                            record.addColumn(new DateColumn(rs.getTime(i)));
                            break;

                        // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                        case Types.DATE:
                            if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                                record.addColumn(new LongColumn(rs.getInt(i)));
                            } else {
                                record.addColumn(new DateColumn(rs.getDate(i)));
                            }
                            break;

                        case Types.TIMESTAMP:
                            record.addColumn(new DateColumn(rs.getTimestamp(i)));
                            break;

                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                            record.addColumn(new BytesColumn(rs.getBytes(i)));
                            break;

                        // warn: bit(1) -> Types.BIT 可使用BoolColumn
                        // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                        case Types.BOOLEAN:
                        case Types.BIT:
                            record.addColumn(new BoolColumn(rs.getBoolean(i)));
                            break;

                        // 针对hive中特有的复杂类型,转为string类型输出
                        case Types.ARRAY:
                        case Types.STRUCT:
                        case Types.JAVA_OBJECT:
                        case Types.OTHER:
                            String arrStr = (String) rs.getObject(i);
                            record.addColumn(new StringColumn(arrStr));
                            break;

                        case Types.NULL:
                            String stringData = null;
                            if(rs.getObject(i) != null) {
                                stringData = rs.getObject(i).toString();
                            }
                            record.addColumn(new StringColumn(stringData));
                            break;

                        default:
                            throw DataXException
                                    .asDataXException(
                                            HiveReaderErrorCode.UNSUPPORTED_TYPE,
                                            String.format(
                                                    "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                    metaData.getColumnName(i),
                                                    metaData.getColumnType(i),
                                                    metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                if (IS_DEBUG) {
                    LOG.debug("read data " + record.toString()
                            + " occur exception:", e);
                }
                //TODO 这里识别为脏数据靠谱吗？
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }

    }
}
