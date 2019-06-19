package org.dean.camel.table;

/**
 * @description: 使用case class和table api进行统计
 * @author: dean
 * @create: 2019/06/19 16:34
 */
object TableWordCount {
  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.TableEnvironment
    import org.apache.flink.types.Row
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val input = environment.fromElements(
      Word("Hello",1),
      Word("Flink",1),
      Word("Hello",2))
    val table = tableEnvironment.fromDataSet(input)
    val result = table.groupBy("word")
      .select("word,word.count as counter")
      .where("counter > 1").orderBy("counter")
    tableEnvironment.toDataSet[Row](result).print()
  }

  case class Word(word:String,count:Int)

}
