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

package com.art4ul.pq.action

import java.io.{OutputStream, PrintStream, PrintWriter}

import com.art4ul.pq.{CmdType, ExecutionContext}
import com.art4ul.pq.outputformat.OutputFormatter
import com.art4ul.pq.parquet.ParquetSupport.ParquetRecord
import com.art4ul.pq.parquet.{MetadataManager, ParquetIO}
import org.apache.hadoop.conf.Configuration

object ContentViewer {

  def lineLimit(implicit ctx: ExecutionContext): Int = {
    ctx.limit.getOrElse {
      throw new RuntimeException("Limit undefined")
    }
  }

  class CatAction(printer: PrintStream)(implicit ctx: ExecutionContext)
    extends ContentViewer(printer, s => s)

  class HeadAction(printer: PrintStream)(implicit ctx: ExecutionContext)
    extends ContentViewer(printer, s => s.take(lineLimit))

  class TailAction(printer: PrintStream)(implicit ctx: ExecutionContext)
    extends ContentViewer(printer, s => s.takeRight(lineLimit))

}


class ContentViewer(printer: PrintStream, fetcher: Stream[ParquetRecord] => Stream[ParquetRecord])
                   (implicit ctx: ExecutionContext) extends Action {


  def action(): Unit = {
    implicit val fsConfig = new Configuration()
    val io = new ParquetIO(fsConfig)
    val schema = MetadataManager.commonSchema(ctx.paths)

    def records = fetcher(io.readParquets(ctx.paths: _*)())

    val formatter = OutputFormatter(schema, printer)
    try {
      formatter.format(records)
    } finally {
      formatter.close()
    }
  }

}
