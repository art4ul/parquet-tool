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

import java.io.PrintStream

import com.art4ul.pq.ExecutionContext
import com.art4ul.pq.parquet.ParquetSupport.ParquetRecord
import org.apache.parquet.schema.MessageType
import com.github.tototoshi.csv._
import scala.collection.JavaConversions._

class CsvOutputFormatter(override val schema: MessageType, out: PrintStream )(implicit conf: ExecutionContext)
  extends OutputFormatter {

  val writer = CSVWriter.open(out)

  def formatRecord(record: ParquetRecord): Unit = {
    val line = schema.getPaths
      .map(col => record(col.toList))
    writer.writeRow(line)
  }

  override def format(stream: => Stream[ParquetRecord]): Unit = {
    val line = schema.getPaths.map(p => p.mkString("."))
    if (!conf.outputFormatOptions.skipHeader) {
      writer.writeRow(line)
    }
    super.format(stream)
  }

  def close(): Unit = {
    writer.close()
  }
}