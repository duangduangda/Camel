package org.dean.camel.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource


/**
 * @description: datastream与table之间的相互转化
 * @author: dean 
 * @create: 2019/06/18 23:28 
 */
object TableDataStreamConverter {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.core.fs.FileSystem.WriteMode
    import org.apache.flink.table.sinks.CsvTableSink
    import org.apache.flink.types.Row

    val streamEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(streamEnvironment)
    val csvTableSource = CsvTableSource
      .builder()
      .path("/Users/dean/communities.csv")
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
    tableEnvironment.registerTableSource("community",csvTableSource)
    val queryResultTable = tableEnvironment
      .sqlQuery("SELECT groupId,groupName FROM community")
    // 控制台打印
    val dataStream = tableEnvironment.toAppendStream[Row](queryResultTable)
    dataStream.print()
    // 输出到外部sink
    val sink = new CsvTableSink("/Users/dean/flink",",",1, WriteMode.OVERWRITE)
    tableEnvironment.registerTableSink("tableSink",Array("groupId","groupName"),Array(Types.STRING,Types.STRING),sink)
//    queryResultTable.writeToSink(sink)
    queryResultTable.insertInto("tableSink")
    streamEnvironment.execute()
  }

}
