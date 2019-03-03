package org.dean.camel

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * 单词统计
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // get input data
    val text =
    // read the text file from given input path
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WORDS: _*)
      }

    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuples) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)

    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")
  }

  val WORDS: Array[String] = Array[String]("To be, or not to be,--that is the question:--", "Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune", "Or to take arms against a sea of troubles,", "And by opposing end them?--To die,--to sleep,--", "No more; and by a sleep to say we end", "The heartache, and the thousand natural shocks", "That flesh is heir to,--'tis a consummation", "Devoutly to be wish'd. To die,--to sleep;--", "To sleep! perchance to dream:--ay, there's the rub;", "For in that sleep of death what dreams may come,", "When we have shuffled off this mortal coil,", "Must give us pause: there's the respect", "That makes calamity of so long life;", "For who would bear the whips and scorns of time,", "The oppressor's wrong, the proud man's contumely,", "The pangs of despis'd love, the law's delay,", "The insolence of office, and the spurns", "That patient merit of the unworthy takes,", "When he himself might his quietus make", "With a bare bodkin? who would these fardels bear,", "To grunt and sweat under a weary life,", "But that the dread of something after death,--", "The undiscover'd country, from whose bourn", "No traveller returns,--puzzles the will,", "And makes us rather bear those ills we have", "Than fly to others that we know not of?", "Thus conscience does make cowards of us all;", "And thus the native hue of resolution", "Is sicklied o'er with the pale cast of thought;", "And enterprises of great pith and moment,", "With this regard, their currents turn awry,", "And lose the name of action.--Soft you now!", "The fair Ophelia!--Nymph, in thy orisons", "Be all my sins remember'd.")
}
