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


import com.art4ul.pq.{ExecutionContext, OutputFormat}
import com.art4ul.pq.parquet.ParquetSupport.ParquetRecord
import org.apache.parquet.schema.MessageType

abstract class OutputFormatter {

  val schema: MessageType

  def formatRecord(record:ParquetRecord):Unit

  def format(stream:  => Stream[ParquetRecord]): Unit = stream.foreach(formatRecord)

  def close():Unit

}

object OutputFormatter {
  def apply(schema: MessageType)(implicit ctx: ExecutionContext): OutputFormatter = ctx.outputFormat match {
    case OutputFormat.Table => new TableOutputFormatter(schema)
    case OutputFormat.Csv => new CsvOutputFormatter(schema)
    case _ => throw new UnsupportedOperationException()
  }
}
