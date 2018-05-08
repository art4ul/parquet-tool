package com.art4ul.pq.outputformat


import com.art4ul.pq.{ExecutionContext, OutputFormat}
import com.art4ul.pq.parquet.ParquetSupport.ParquetRecord
import org.apache.parquet.schema.MessageType

abstract class OutputFormatter {

  val schema: MessageType

  def formatRecord(record:ParquetRecord):Unit

  def format(stream: Stream[ParquetRecord]): Unit = stream.foreach(formatRecord)

}

object OutputFormatter {
  def apply(schema: MessageType)(implicit ctx: ExecutionContext): OutputFormatter = ctx.outputFormat match {
    case OutputFormat.Table => new TableOutputFormatter(schema)
    case OutputFormat.Csv => new CsvOutputFormatter(schema)
    case _ => throw new UnsupportedOperationException()
  }
}
