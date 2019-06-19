package org.dean.camel.table

import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}





/**
  * @description: dataset与table之间的相互转化
  * @author: dean
  * @create: 2019/06/14 21:57
  */
object TableDataSetConverter {
  import org.apache.flink.table.api.scala.BatchTableEnvironment

  def runSqlQuery(tableEnvironment: BatchTableEnvironment): Unit = {
    val queryResultTable = tableEnvironment
      .sqlQuery("SELECT groupId as Id,groupName as name,imageUrl as url FROM community")
    // 按行输出
    tableEnvironment.toDataSet[Row](queryResultTable).print()
    // 按照形如（String,String,String）的样式输出
    tableEnvironment.toDataSet[(String,String,String)](queryResultTable).print()
  }

  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val csvTableSource = CsvTableSource
      .builder()
      .path("/Users/yaohua.dong/communities.csv")
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
//    runSqlQuery(tableEnvironment)
    val community = tableEnvironment.scan("community")
    val result = community
        .where("groupId === '@TGS#2MKNBPYFX'")
//      .select("groupId as id,groupName as name")
    tableEnvironment.toDataSet[Row](result).print()
  }

}
