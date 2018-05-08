package com.art4ul.pq.outputformat

import java.io.PrintStream

import com.art4ul.pq.ExecutionContext
import com.art4ul.pq.parquet.ParquetSupport.ParquetRecord
import org.apache.parquet.schema.MessageType
import scala.collection.JavaConversions._

class TableOutputFormatter(override val schema: MessageType, out: PrintStream = System.out)(implicit conf: ExecutionContext)
  extends OutputFormatter {


  def printLine(line: Seq[String]): Unit = {
    out.print("|\t")
    out.print(line.mkString("\t|\t"))
    out.println("\t|")
    //out.println("-------------------------")
  }

  def formatRecord(record: ParquetRecord): Unit = {
    val line = schema.getPaths
      .map(col => record.get(col.toList).getOrElse("null").toString)
    printLine(line)
  }

  override def format(stream: Stream[ParquetRecord]): Unit = {
    val line = schema.getPaths.map(p => p.mkString("."))
    printLine(line)
    super.format(stream)
  }
}
