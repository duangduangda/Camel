package org.dean.camel.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.sources._

/**
  * @description: datastream与table之间的相互转化
  * @author: dean
  * @create: 2019/06/14 21:57
  */
object TableDataSetConverter {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.types.Row
    val environment = ExecutionEnvironment.getExecutionEnvironment
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
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    tableEnvironment.registerTableSource("community",csvTableSource)
    val queryResultTable = tableEnvironment
      .sqlQuery("SELECT groupId,groupName,imageUrl FROM community")
    // 按行输出
    tableEnvironment.toDataSet[Row](queryResultTable).print()
    // 按照形如（String,String,String）的样式输出
//    tableEnvironment.toDataSet[(String,String,String)](queryResultTable).print()
  }

}
