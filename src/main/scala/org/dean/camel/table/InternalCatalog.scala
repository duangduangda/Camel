package org.dean.camel.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
  * InternalCatalog 注册TableSource
  *
  * @author dean
  * @since 2019-07-17
  */
object InternalCatalog {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val csvTableSource = CsvTableSource.builder()
      .path("D:\\test.csv")
      .fieldDelimiter(",")
      .lineDelimiter("\n")
      .field("id",Types.INT)
      .field("name",Types.STRING)
      .field("age",Types.INT)
      .ignoreFirstLine
      .build
    tableEnvironment.registerTableSource("t_friend",csvTableSource)
    val table = tableEnvironment.sqlQuery("select * from t_friend").where("age > 20")
    tableEnvironment.toAppendStream[Row](table).print
    environment.execute("InternalCatalog running")
  }
}
