/*
 * Copyright (C) 2018 Artsem Semianenka (http://art4ul.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.art4ul.pq

import Constants._
import com.art4ul.pq.OutputFormat.OutputFormat
import org.apache.hadoop.fs.Path

object OutputFormat extends Enumeration {
  type OutputFormat = Value
  val Csv, Table, Json = Value
}

object CmdType extends Enumeration {
  type CmdType = Value
  val Schema, Cat, Head, Tail, Meta = Value
}

case class OutputFormatOptions(skipHeader: Boolean = false)

case class ExecutionContext(cmd: CmdType.Value = CmdType.Head,
                            query: Option[String] = None,
                            limit: Option[Int] = Some(DefaultLineLimit),
                            verbose: Boolean = false,
                            options: Map[String, String] = Map[String, String](),
                            outputFormat: OutputFormat = OutputFormat.Table,
                            outputFormatOptions: OutputFormatOptions = OutputFormatOptions(),
                            paths: Seq[Path] = Seq())


object ExecutionContext {

  implicit val pathReader: scopt.Read[Path] = scopt.Read.reads(new Path(_))
  implicit val OutputFormatRead: scopt.Read[OutputFormat.Value] =
    scopt.Read.reads(OutputFormat withName _)

  def withConfig(args: Array[String])(fun: ExecutionContext => Unit): Unit = {

    val parser = new scopt.OptionParser[ExecutionContext]("parquet-tool") {
      head("Parquet tool", "1.0")

      cmd("cat")
        .optional()
        .action((_, c) => c.copy(cmd = CmdType.Cat, limit = None))
        .text("Print full content of parquet file")

      cmd("head")
        .optional()
        .action((_, c) => c.copy(cmd = CmdType.Head))
        .text(s"Print first N line from parquet file (Default: $DefaultLineLimit)")

      cmd("tail")
        .optional()
        .action((_, c) => c.copy(cmd = CmdType.Tail))
        .text(s"Print last N line from parquet file (Default: $DefaultLineLimit)")

      cmd("schema")
        .optional()
        .action((_, c) => c.copy(cmd = CmdType.Schema))
        .text(s"Show parquet schema")

      cmd("meta")
        .optional()
        .action((_, c) => c.copy(cmd = CmdType.Meta))
        .text(s"Show parquet metadata")

      opt[String]('q', "query")
        .action((x, c) => c.copy(query = Some(x)))
        .text("SQL like query")

      opt[Unit]("verbose")
        .action((_, c) => c.copy(verbose = true))
        .text("verbose is a flag")

      opt[Int]('l',"limit")
        .action((l, c) => c.copy(limit = Some(l)))
        .text("Limit of lines")

      opt[String]('f', "format")
        .action((f, c) => f.toLowerCase match {
          case "csv" => c.copy(outputFormat = OutputFormat.Csv)
          case "table" => c.copy(outputFormat = OutputFormat.Table)
          case "json" => c.copy(outputFormat = OutputFormat.Json)
          case _ => c.copy(outputFormat = OutputFormat.Table)
        })
        .text("Set output format: <csv , table, json>")

      opt[Unit]('H', "skipheader")
        .action((_, c) => c.copy(outputFormatOptions = c.outputFormatOptions.copy(skipHeader = true)))
        .text("Skip header")

      opt[(String, String)]('o', "options")
        .action({
          case ((k, v), c) => c.copy(options = c.options + (k -> v))
        }).
        keyValueName("<key>", "<value>").
        text("Additional options")

      help("help")
        .text("prints this usage text")

      arg[Path]("<file>...")
        .unbounded()
        .required()
        .action((x, c) => c.copy(paths = c.paths :+ x))
        .text("Files")

    }

    val ctx = ExecutionContext()
    parser.parse(args, ctx).foreach(fun)
  }

}
