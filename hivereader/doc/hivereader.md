# DataX HiveReader 插件文档

------------

## 1 快速介绍

HiveReader提供了读取Hive数据仓库数据存储的能力，支持全表读取和自定义SQL查询两种读取数据形式。在底层实现上，全表读取与HdfsReader方式一致：会获取分布式文件系统上文件的数据，并转换为DataX传输协议传递给Writer。自定义SQL查询底层通过JDBC连接远程Hive数据仓库，并根据用户配置的SQL语句进行查询，将查询结果转化为DataX自定义的数据类型传递给下游Writer处理。

**目前HiveReader中全量同步形式支持的文件格式有textfile（text）、orcfile（orc）、rcfile（rc）、sequence file（seq）和普通逻辑二维表（csv）类型格式的文件，且文件内容存放的必须是一张逻辑意义上的二维表。自定义SQL的部分表数据同步无以上要求。**

**HiveReader需要Jdk1.7及以上版本的支持。**


## 2 功能与限制

HiveReader实现了从Hive数据仓库读取全量数据以及通过自定义SQL语句读取部分数据，并将其转为DataX协议的功能。textfile是Hive建表时默认使用的存储格式，数据不做压缩，本质上textfile就是以文本的形式将数据存放在hdfs中，对于DataX而言，HiveReader中全量同步的实现上类比TxtFileReader，有诸多相似之处。orcfile，它的全名是Optimized Row Columnar file，是对RCFile做了优化。据官方文档介绍，这种文件格式可以提供一种高效的方法来存储Hive数据。HiveReader在进行全量同步时利用Hive提供的OrcSerde类，读取解析orcfile文件的数据。目前HiveReader支持的功能如下：

1. **全量同步：**
   1. 支持textfile、orcfile、rcfile、sequence file和csv格式的文件，且要求文件内容存放的是一张逻辑意义上的二维表。
   2. 支持多种类型数据读取(使用String表示)，支持列裁剪，支持列常量
   3. 支持递归读取、支持正则表达式（"*"和"?"）。
   4. 支持orcfile数据压缩，目前支持SNAPPY，ZLIB两种压缩方式。
   5. 多个File可以支持并发读取。
   6. 支持sequence file数据压缩，目前支持lzo压缩方式。
   7. csv类型支持压缩格式有：gzip、bz2、zip、lzo、lzo_deflate、snappy。
   8. 目前插件中Hive版本为1.1.1，Hadoop版本为2.7.1（Apache［为适配JDK1.7］,在Hadoop 2.5.0, Hadoop 2.6.0 和Hive 1.2.0测试环境中写入正常；其它版本需后期进一步测试； 
   9. 支持kerberos认证（注意：如果用户需要进行kerberos认证，那么用户使用的Hadoop集群版本需要和hdfsreader的Hadoop版本保持一致，如果高于hdfsreader的Hadoop版本，不保证kerberos认证有效）
2. **自定义SQL，部分表数据同步**
   1. 支持以传参形式传入自定义SQL语句，进行部分表数据的读取与同步。
   2. 支持一次性传递多条SQL语句（但需保证查询结果的表结构一致）。
   3. 目前插件中hive-jdbc版本为1.1.1，兼容在这之上高版本的HiveServer2

我们暂时不能做到：

1. 单个File支持多线程并发读取，这里涉及到单个File内部切分算法。二期考虑支持。
2. 目前还不支持hdfs HA;



## 3 功能说明


### 3.1 配置样例

```json
{
    "job": {
        "setting": {
            "speed": {
                "channel": 3
            }
        },
        "content": [
            {
                "reader": {
                    "name": "hivereader",
                    "parameter": {
                        "defaultFS": "hdfs://xxx:port",
                        // 以下为全表同步形式所需参数
                        "hiveRoot": "/user/hive/warehouse/*",
                        "tables": [
                             "table1",
                             "table2"
                        ],
                        "fileType": "orc",
                        "encoding": "UTF-8",
                        "fieldDelimiter": ",",
                        // 以下为部分表数据同步形式所需参数
                        "sqls": [
                            "select * from table1 where condition=1",
                            "select * from table2 where condition=2"
                        ],
                        "jdbcUrl": "jdbc:hive2://ip:port",
                        "user": "hive",
                        "password": ""
                    }

                },
                "writer": {
                    "name": "streamwriter",
                    "parameter": {
                        "print": true
                    }
                }
            }
        ]
    }
}
```

### 3.2 参数说明（各个配置项值前后不允许有空格）

* **defaultFS**

	* 描述：Hadoop hdfs文件系统namenode节点地址。 <br />目前HdfsReader已经支持Kerberos认证，如果需要权限认证，则需要用户配置kerberos参数，见下面
	* 必选：是 <br />
	* 默认值：无 <br />
	
	> **注：关于HiveReader中的同步形式，必须从全表同步和部分表同步形式中选择一种。程序判定是部分表同步形式的标准为sqls参数不为null。即sqls参数有传值则被识别为自定义SQL的部分表同步数据形式，反之sqls参数未传值则说明为全表同步形式。**
	>
	> **以下参数的必选与否均建立在是否为对应同步形式的基础上：**
	>
	> e.g hiveRoot参数在全量同步时为必传参数

* **hiveRoot**

	- 描述：Hive数据仓库在HDFS文件系统中的根目录路径。 <br />
- 必选：是 <br />
	- 默认值：无 <br />

* **tables**

  * 描述：数组类型，需要从Hive数据仓库中同步的表名，可传多张表。
  * 必选：是
  * 默认值：无

* **fileType**

	* 描述：文件的类型，目前只支持用户配置为"text"、"orc"、"rc"、"seq"、"csv"。 <br />

		text表示textfile文件格式

		orc表示orcfile文件格式
		
		rc表示rcfile文件格式
		
		seq表示sequence file文件格式
		
		csv表示普通hdfs文件格式（逻辑二维表）

		**特别需要注意的是，HiveReader能够自动识别文件是orcfile、textfile或者还是其它类型的文件，但该项是必填项，HiveReader则会只读取用户配置的类型的文件，忽略路径下其他格式的文件**

		**另外需要注意的是，由于textfile和orcfile是两种完全不同的文件格式，所以HiveReader对这两种文件的解析方式也存在差异，这种差异导致hive支持的复杂复合类型(比如map,array,struct,union)在转换为DataX支持的String类型时，转换的结果格式略有差异，比如以map类型为例：**

		orcfile map类型经hdfsreader解析转换成datax支持的string类型后，结果为"{job=80, team=60, person=70}"

		textfile map类型经hdfsreader解析转换成datax支持的string类型后，结果为"job:80,team:60,person:70"

		从上面的转换结果可以看出，数据本身没有变化，但是表示的格式略有差异，所以如果用户配置的文件路径中要同步的字段在Hive中是复合类型的话，建议配置统一的文件格式。

		**如果需要统一复合类型解析出来的格式，我们建议用户在hive客户端将textfile格式的表导成orcfile格式的表**

	* 必选：是 <br />

	* 默认值：无 <br />


* **fieldDelimiter**

	* 描述：读取的字段分隔符 <br />

	  **另外需要注意的是，HiveReader在读取textfile数据时，需要指定字段分割符，如果不指定默认为','，HiveReader在读取orcfile时，用户无需指定字段分割符**

	* 必选：否 <br />

	* 默认值：, <br />


* **encoding**
  * 描述：读取文件的编码配置。<br />

   	* 必选：否 <br />

   	* 默认值：utf-8 <br />


* **nullFormat**

  * 描述：文本文件中无法使用标准字符串定义null(空指针)，DataX提供nullFormat定义哪些字符串可以表示为null。<br />

  	 例如如果用户配置: nullFormat:"\\N"，那么如果源头数据是"\N"，DataX视作null字段。
  	
   	* 必选：否 <br />

    * 默认值：无 <br />

* **haveKerberos**

  * 描述：是否有Kerberos认证，默认false<br />

  	 例如如果用户配置true，则配置项kerberosKeytabFilePath，kerberosPrincipal为必填。
  	
   	* 必选：haveKerberos 为true必选 <br />

    * 默认值：false <br />

* **kerberosKeytabFilePath**

  * 描述：Kerberos认证 keytab文件路径，绝对路径<br />

   	* 必选：否 <br />

    * 默认值：无 <br />

* **kerberosPrincipal**

  * 描述：Kerberos认证Principal名，如xxxx/hadoopclient@xxx.xxx <br />

   	* 必选：haveKerberos 为true必选 <br />

    * 默认值：无 <br />

* **compress**

  * 描述：当fileType（文件类型）为csv下的文件压缩方式，目前仅支持 gzip、bz2、zip、lzo、lzo_deflate、hadoop-snappy、framing-snappy压缩；**值得注意的是，lzo存在两种压缩格式：lzo和lzo_deflate，用户在配置的时候需要留心，不要配错了；另外，由于snappy目前没有统一的stream format，datax目前只支持最主流的两种：hadoop-snappy（hadoop上的snappy stream format）和framing-snappy（google建议的snappy stream format）**;orc文件类型下无需填写。<br />

   	* 必选：否 <br />

    * 默认值：无 <br />

* **hadoopConfig**

  * 描述：hadoopConfig里可以配置与Hadoop相关的一些高级参数，比如HA的配置。<br />

  	```json
  	"hadoopConfig":{
  	        "dfs.nameservices": "testDfs",
  	        "dfs.ha.namenodes.testDfs": "namenode1,namenode2",
  	        "dfs.namenode.rpc-address.aliDfs.namenode1": "",
  	        "dfs.namenode.rpc-address.aliDfs.namenode2": "",
  	        "dfs.client.failover.proxy.provider.testDfs": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
  	}
  	```

  * 必选：否 <br />

    * 默认值：无 <br />

* **csvReaderConfig**

  * 描述：读取CSV类型文件参数配置，Map类型。读取CSV类型文件使用的CsvReader进行读取，会有很多配置，不配置则使用默认值。<br />

   	* 必选：否 <br />

    * 默认值：无 <br />

     

常见配置：

```json
"csvReaderConfig":{
        "safetySwitch": false,
        "skipEmptyRecords": false,
        "useTextQualifier": false
}
```

所有配置项及默认值,配置时 csvReaderConfig 的map中请**严格按照以下字段名字进行配置**：

```
boolean caseSensitive = true;
char textQualifier = 34;
boolean trimWhitespace = true;
boolean useTextQualifier = true;//是否使用csv转义字符
char delimiter = 44;//分隔符
char recordDelimiter = 0;
char comment = 35;
boolean useComments = false;
int escapeMode = 1;
boolean safetySwitch = true;//单列长度是否限制100000字符
boolean skipEmptyRecords = true;//是否跳过空行
boolean captureRawRecord = true;
```

- **sqls**
  - 描述：用户自定义SQL查询语句，用于查询表中部分数据进行同步。数组类型，支持传递多条SQL语句<br />
  - 必选：是 <br />
  - 默认值：无 <br />
- **jdbcUrl**
  - 描述：用于连接HiveServer2的jdbc连接url<br />
  - 必选：是 <br />
  - 默认值：无 <br />
- **user**
  - 描述：用于连接HiveServer2的用户名<br />
  - 必选：是 <br />
  - 默认值：无 <br />
- **password**
  - 描述：用于连接HiveServer2的密码<br />
  - 必选：是 <br />
  - 默认值：无 <br />

### 3.3 类型转换

HiveReader提供了类型转换的建议表如下：

| DataX 内部类型| Hive表 数据类型    |
| -------- | -----  |
| Long     |TINYINT,SMALLINT,INT,BIGINT|
| Double   |FLOAT,DOUBLE|
| String   |String,CHAR,VARCHAR,STRUCT,MAP,ARRAY,UNION,BINARY|
| Boolean  |BOOLEAN|
| Date     |Date,TIMESTAMP|

其中：

* Long是指Hdfs文件文本中使用整形的字符串表示形式，例如"123456789"。
* Double是指Hdfs文件文本中使用Double的字符串表示形式，例如"3.1415"。
* Boolean是指Hdfs文件文本中使用Boolean的字符串表示形式，例如"true"、"false"。不区分大小写。
* Date是指Hdfs文件文本中使用Date的字符串表示形式，例如"2014-12-31"。

特别提醒：

* Hive支持的数据类型TIMESTAMP可以精确到纳秒级别，所以textfile、orcfile中TIMESTAMP存放的数据类似于"2015-08-21 22:40:47.397898389"，转化为DataX的Date会导致纳秒部分丢失。


## 4 性能报告



## 5 约束限制

### 	5.1 SQL安全性

HiveReader提供querySql语句交给用户自己实现SELECT抽取语句，HiveReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ

### 	6.1 hive与hive-jdbc版本匹配问题

若Hive为CDH版本，在使用HiveReader中的部分表数据同步功能时，其hive-jdbc也应与Hive保持一致同为CDH版本。

