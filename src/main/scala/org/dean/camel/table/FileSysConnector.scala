package org.dean.camel.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row

/**
  * @author dean
  * @since 2019-07-17
  */
object FileSysConnector {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val schema = new Schema()
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("age", Types.INT)
    tableEnvironment.connect(
      new FileSystem()
        .path("D:\\test.csv"))
      .withFormat(new Csv()
      .fieldDelimiter(",")
      .lineDelimiter("\n")
      .field("id", Types.INT)
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .ignoreFirstLine
      .ignoreParseErrors)
      .withSchema(schema)
      .registerTableSource("t_friend")

    val table = tableEnvironment.scan("t_friend")
      .select("name,age")
      .where("age > 10")
      .orderBy("age")

    tableEnvironment.toDataSet[Row](table).print()
  }
}
