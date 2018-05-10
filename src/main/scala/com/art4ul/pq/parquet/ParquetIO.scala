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

package com.art4ul.pq.parquet

import com.art4ul.pq.parquet.ParquetSupport.{ParquetReadSupport, ParquetRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.compat.FilterCompat
import org.apache.parquet.hadoop.ParquetWriter.{DEFAULT_BLOCK_SIZE, DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_PAGE_SIZE}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.schema.MessageType
import org.slf4j.LoggerFactory

class ParquetIO(conf: Configuration) {

  import ParquetIO._

  val log = LoggerFactory.getLogger(getClass)

  def readParquet(path: Path, filter: FilterCompat.Filter = FilterCompat.NOOP): Stream[ParquetRecord] = {
    log.info(s"read parquet:$path")
    ParquetReader
      .builder(new ParquetReadSupport, path)
      .withConf(conf)
      .withFilter(filter)
      .build()
      .toStream
  }

  def readParquets(path: Path*)(filter: FilterCompat.Filter = FilterCompat.NOOP): Stream[ParquetRecord] = {
    path.toList match {
      case Nil => Stream.empty[ParquetRecord]
      case head :: Nil => readParquet(head, filter)
      case head :: tail => tail.foldRight(readParquet(head))((el, acc) => acc.append(readParquet(el)))
    }
  }

//  def writeParquet(path: Path, schema: MessageType, data: => Stream[ParquetRecord]): Unit = {
//    val writer = new ParquetWriter(path, new ParquetWriteSupport(schema),
//      CompressionCodecName.SNAPPY, DEFAULT_BLOCK_SIZE,
//      DEFAULT_PAGE_SIZE,
//      DEFAULT_IS_DICTIONARY_ENABLED,
//      DEFAULT_IS_VALIDATING_ENABLED)
//
//    data.foreach(elem => writer.write(elem))
//    writer.close()
//  }
}

object ParquetIO{

  implicit class ParquetReaderStream[T](parquetReader: ParquetReader[T]) {
    def toStream: Stream[T] = {
      val elem = parquetReader.read
      if (elem != null) {
        Stream.cons(elem, toStream)
      } else {
        parquetReader.close()
        Stream.Empty
      }
    }
  }
}
