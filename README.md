# hadoop_hbase_groupby
my first homework of Bigdata and Analysis lession

[TOC]

## 问题描述

作业一：大数据存储系统编程

1. 作业提交格式
	- 文件命名：组号_学号_hw1.java
2. 总体任务
	- 从HDFS中读取文件，然后经过中间处理，每个人处理任务不同，最后写入HBase
3. 中间处理
	- 分类：
		+ join：
			* Hash join
			* Sort-merge join
		+ group-by：
			* Hash based group-by
			* Sort based group-by
		+ distinct：
			* Hash based distinct
			* Sort based distinct
	- Group-by：
		+ 我的任务是hash based group-by
		+ 输入：/hw1/lineitem.tbl
		+ 输出：
			* 1-多列
			* count，avg(Ri)，max(Ri)
			* count和max为准确值，avg保留小数点后两位
4. 写入HBase
	- 示例：
		+ 命令行：java Hw1Grp2 R=/hw1/lineitem.tbl groupby:R2 res:count,avg(R3),max(R4)
		+ 输出HBase
			* 数据模型：
				- ```<row key, column family:column key, version, value>```
				- key = row key + column
				- column family种类有限
				- column key可以很多
				- row key顺序存储
				- column family将分开存储，类似列式数据库(投影高效)
			* 包含：
				* (row key=abc, rec:count=3)(row key=abc, res:avg(R3)=10)(row key=abc, res:max(R4)=20)

5. 补充知识

	| row key | time stamp | ColumnFamily contents | ColumnFamily anchor |
	| --- | --- | --- | --- |
	| "com.cnn.www" | t9 |  | anchor:cnnsi.com="CNN" |
	| "com.cnn.www" | t8 |  | anchor:my.look.ca="CNN.com" |
	| "com.cnn.www" | t6 | contents:html="<html>..." |  |
	| "com.cnn.www" | t5 | contents:html="<html>..." |  |

	 - 概念视图：
	 	- 稀疏的行的集合：webtable表：
	 	- 包含1行：检索记录的主键
	 	- 包含两个列族：contents和anchor
	 		+ anchor有两个列；contents只有一列；
	 	- 1列：列族+修饰符：
	 		+ contents:html
	 		+ anchor:cnnsi.com
	 	- 每个列族可有任意多个列；每个列可以包含任意多版本；没有插入过数据的列式空；
	 	- 列族中的列是排序过的，并且一起存储；
	 	- time stamp时间戳：version，一般是系统时间，ms
	 	- HBase以字节数组存储；
	 - 物理视图：
	 	- 按列存储；
	 	- 好处：增加新列族，不需对物理存储做任何调整；
	 	- 1个概念视图 经过映射= 2个物理视图

			| row key | time stamp | Column Family anchor |
			| --- | --- | --- |
			| "com.cnn.www" | t9 | anchor:cnnsi.com="CNN" |

			| row key | time stamp | ColumnFamily contents |
			| --- | --- | --- | 
			| "com.cnn.www" | t5 | contents:html="<html>..." |

	 - 最简：
	 	+ (row, family:column, timestamp)=value

			| row key | ColumnFamily |
			| ---- | ---- |
			| key | (timestamp, columnfamily:qualifier=value) |

## 具体实现

1. 环境配置
	- Ubuntu14.04.2
	- jdk.1.7
	- hadoop2.6
	- hbase0.98
	- 环境配置网上都有，自行Google了。
2. hadoop操作
	- 启动hadoop：sh sbin/start-all.sh
	- 启动hdfs：start-dfs.sh
	- 启动hbase：start-hbase.sh
	- 停止hbase：stop-hbase.sh
	- 停止hdfs：stop-dfs.sh
3. 从HDFS读取文件
	- HDFS
		+ 文件格式：
			* 文本文件
			* 每行是一个关系型记录
			* 每列用|分开
			* 例如：1 | A M E R I C A | h suseironic,evenrequests.s|
		+ HDFS命令：
		 	* 列出目录：hadoop fs -ls / 
			* 列出所有文件：hadoop fs -ls -R /
			* 创建：hadoop fs -mkdir /yu ; 自动创建父目录，带-p
			* 删除：hadoop fs -rm -r /yu ;
			* 复制: hadoop fs -cp /hw1 /yu ;
			* 移动: hadoop fs -mv /hw1 /yu ;
			* 详情: hadoop fs -count /hw1 ;
			* 大小: hadoop fs -du -h /hw1 ;
			* 上传本地文件：hdfs dfs -put /home/guest/work/a.txt /hw2/
	- 程序实现：
```
public class HDFSTest{
	public static void main(String[] args) throws IOException, URISyntaxException{
		String file = "hdfs://localhost:9000/文件路径";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path =new Path(file);
		FSDataInputStream in_stream =fs.open(path);
		BufferedReader in= newBufferedReader(new InputStreamReader(in_stream));
		Strings;
		while((s=in.readLine()) != null){
			System.out.println(s);
		}
		in.close();
		fs.close();
	}
}
```
3. Group-by实现
	- Hash based
		+ 建立一个hash table
		+ key = group by key
		+ value = 需要统计的信息
			* count：当前计数
			* avg：当前均值
			* max：当前最大值
		+ 把输入都使用hash table完成统计，最后扫描输出所有hash table中的所有项
	- Sort based
		+ 根据group by key排序
		+ 然后同一个group的都会在一起
		+ 统计输出

4. 写入HBase
	- HBase shell：
		+ 查询服务器状态：status 
		+ 查看HBase：version
		+ 查看所有表：list
		+ 查看表结构：describe 'user'
		+ 创建表：create 'mytable', 'mycf'
		+ put插入: put 'mytable', 'abc', 'mycf:a', '123'
		+ get获得: get 'mytable', 'abc'
		+ scan: scan 'mytable'
		+ disable失效：disable 'mytable' ; 修改表结构、删除时，需要先执行此命令
		+ enable使能：enable 'mytable' ; 失效后需要使用
		+ alter修改表结构：
			* disable 'mytable'
			* alter 'mytable', 'mycf2'
			* enable 'mytable'
		+ 重命名列族：只能先创建新列族，复制原数据，删除原列族
		+ 删除：
			* 删除列族：
				- disable 'mytable'
				- alter 'mytable',{NAME=>'mycf2',METHOD=>'delete'}
				- enable 'mytable' 
			* 删除整行：deleteall 'user', 'row1'
			* 删除指定key和族的列：delete 'mytable','row','mycf:a'
			* 删除指定key：delete 'user', 'row','mycf'
			* 删除所有数据：truncate 'mytable'
		+ count行数: count 'mytable'
	- 程序实现：
```
public class HBaseTest {
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		//createtabledescriptor
		String tableName = "mytable";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

		//createcolumndescriptor
		HColumnDescriptor cf = new HColumnDescriptor("mycf");
		htd.addFamily(cf);

		//configureHBase
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);
		hAdmin.createTable(htd);
		hAdmin.close();

		//put"mytable","abc","mycf:a","789"
		HTable table= new HTable(configuration,tableName);
		Putput = newPut("abc".getBytes());
		put.add("mycf".getBytes(),"a".getBytes(),"789".getBytes());
		table.put(put);
		table.close();
		System.out.println("putsuccessfully");
	}
}
```

## 严重错误
1. avg精度：注意avg是小数后两位，count和max都是准确值
2. 输出表名：Result
3. 输出表项：avg(R4)，我把R漏掉了，最后得了0分，相当于总分变为了90，，，老师很严格得用脚本改我们的程序，挺好的，逼格很令我佩服，也算是一个深刻的教训了，注意细节，大家引以为戒~~

## 程序源码
第一次用Java，写的略乱，详见：https://github.com/a550461053/hadoop_hbase_groupby/
