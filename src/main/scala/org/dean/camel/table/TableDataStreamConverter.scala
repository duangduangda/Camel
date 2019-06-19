package org.dean.camel.table

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @description: datastream与table之间的相互转化
  * @author: dean
  * @create: 2019/06/18 23:28
  */
object TableDataStreamConverter {
  /**
    * 创建数据源
    *
    * @param csv
    * @return
    */
  def createTableSource(csv: String) = {
    CsvTableSource
      .builder()
      .path(csv)
      .field("groupId", Types.STRING)
      .field("groupName", Types.STRING)
      .field("creatorAccountId", Types.STRING)
      .field("createTime", Types.STRING)
      .field("imageUrl", Types.STRING)
      .field("deleteTime", Types.STRING)
      .ignoreFirstLine()
      .lineDelimiter("\n")
      .fieldDelimiter(",")
      .ignoreParseErrors()
      .build()
  }

  /**
    *  控制台打印
    *
    * @param queryResultTable
    * @param tableEnvironment
    * @return
    */
  def print2Console(queryResultTable: Table,
                    tableEnvironment: StreamTableEnvironment) = {
    val dataStream = tableEnvironment.toAppendStream[Row](queryResultTable)
    dataStream.print()
  }

  /**
    * 输出到csv文件
    *
    * @param queryResultTable
    * @param tableEnvironment
    * @param sink
    */
  def sink2Csv(queryResultTable: Table,
               tableEnvironment: StreamTableEnvironment,
               sink: CsvTableSink) = {
    // 注册输出端
    tableEnvironment.registerTableSink(
      "tableSink",
      Array("groupId", "groupName"),
      Array(Types.STRING, Types.STRING),
      sink
    )
    // 执行输出
    queryResultTable.insertInto("tableSink")
  }

  def main(args: Array[String]): Unit = {
    val streamEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment =
      TableEnvironment.getTableEnvironment(streamEnvironment)
    // 定义数据源
    val csvTableSource = createTableSource("/Users/dean/communities.csv")
    // 注册数据源
    tableEnvironment.registerTableSource("community", csvTableSource)
    // 执行sql查询
    val queryResultTable = tableEnvironment
      .sqlQuery("SELECT groupId,groupName FROM community")
    // 控制台打印
    print2Console(queryResultTable, tableEnvironment)
    // 定义输出端
    val sink =
      new CsvTableSink("/Users/dean/flink", ",", 1, WriteMode.OVERWRITE)
    // 输出到csv
    sink2Csv(queryResultTable, tableEnvironment, sink)
    // 使用scan,select
    tableEnvironment
      .scan("community")
      .select("*")
      .writeToSink(sink)
    streamEnvironment.execute()
  }

}
