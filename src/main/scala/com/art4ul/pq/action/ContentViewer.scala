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
