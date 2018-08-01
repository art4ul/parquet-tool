package com.art4ul.pq.action

import java.io.PrintStream
import java.lang.StringBuilder
import com.art4ul.pq.ExecutionContext
import com.art4ul.pq.parquet.{MetadataManager, ParquetIO}
import org.apache.hadoop.conf.Configuration

class SchemaAction(printer: PrintStream)(implicit ctx: ExecutionContext) extends Action {

  override def action(): Unit = {
    implicit val fsConfig = new Configuration()
    val schemas = MetadataManager.readSchemas(ctx.paths)
    val sb= new StringBuilder()
    schemas.foreach{schema=>
      schema.writeToStringBuilder(sb,"")
      printer.println(sb.toString)
      printer.println()
    }
  }
}
