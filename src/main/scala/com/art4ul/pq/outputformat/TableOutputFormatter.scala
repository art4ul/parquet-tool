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

package com.art4ul.pq.outputformat

import java.io.{OutputStream, PrintStream, PrintWriter}

import com.art4ul.pq.ExecutionContext
import com.art4ul.pq.parquet.ParquetSupport.ParquetRecord
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._
import de.vandermeer.asciitable.AsciiTable


class TableOutputFormatter(override val schema: MessageType, out: PrintStream)(implicit conf: ExecutionContext)
  extends OutputFormatter {

  val table = new AsciiTable

  def formatRecord(record: ParquetRecord): Unit = {
    val line = schema.getPaths
      .map(col => record.get(col.toList).getOrElse("null").toString)
    table.addRow(line: _*)
    table.addRule
  }

  override def format(stream: => Stream[ParquetRecord]): Unit = {
    table.addRule
    if (!conf.outputFormatOptions.skipHeader) {
      table.addRow(schema.getPaths.map(p => p.mkString(".")): _*)
      table.addRule()
    }
    super.format(stream)
    out.println(table.render())
  }

  override def close(): Unit = {}
}
