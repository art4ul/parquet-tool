package com.art4ul.pq

import com.art4ul.pq.OutputFormat.OutputFormat
import org.apache.hadoop.fs.Path

case class ExecutionContext(query: Option[String] = None,
                            verbose: Boolean = false,
                            outputFormat: OutputFormat = OutputFormat.Table,
                            paths: Seq[Path] = Seq())

object OutputFormat extends Enumeration {
  type OutputFormat = Value
  val Csv,Table,Json = Value
}

object ExecutionContext {

  implicit val pathReader: scopt.Read[Path] = scopt.Read.reads(new Path(_))
  implicit val OutputFormatRead: scopt.Read[OutputFormat.Value] =
    scopt.Read.reads(OutputFormat withName _)

  def withConfig(args: Array[String])(fun: ExecutionContext => Unit):Unit = {
    val parser = new scopt.OptionParser[ExecutionContext]("parquet-tool") {
      head("Parquet tool", "1.0")

      opt[String]('q', "query")
        .action((x, c) => c.copy(query = Some(x)))
        .text("SQL like query")

      opt[Unit]("verbose")
        .action((_, c) => c.copy(verbose = true))
        .text("verbose is a flag")

      opt[Unit]("csv")
        .action((_, c) => c.copy(outputFormat = OutputFormat.Csv))
        .text("CSV Output format")

      help("help")
        .text("prints this usage text")

      arg[Path]("<file>...")
        .unbounded()
        .required()
        .action((x, c) => c.copy(paths = c.paths :+ x))
        .text("Files")

    }

    parser.parse(args, ExecutionContext()).foreach(fun)
  }

}
