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

import java.nio.charset.Charset

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter}
import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, Type}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by artsemsemianenka on 7/1/16.
  */
class ParquetRecordConverter(schema: MessageType) extends GroupConverter {

  private var buffer = new mutable.HashMap[List[String], Any]()

  private val converters: Array[Converter] = {
    schema.getColumns
      .map(getConverter)
      .toArray
  }

  def getCurrentRecord: Map[List[String], Any] = buffer.toMap

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  override def end(): Unit = {}

  override def start(): Unit = {
    buffer = new mutable.HashMap[List[String], Any]()
  }

  def getConverter(columnDesc:ColumnDescriptor): Converter = {
    new ParquetPrimitiveConverter(columnDesc.getPath.toList)
//    if (field.isPrimitive) {
//      new ParquetPrimitiveConverter(field.getName)
////      field.getOriginalType match {
////        case UTF8 => new ParquetStringConverter(field.getName)
////        case null => new ParquetPrimitiveConverter(field.getName)
////        case t => throw new ParquetEncodingException(s"Unsuported type: $t")
////      }
//
//    } else throw new ParquetEncodingException("Unsuported type")
  }

  class ParquetPrimitiveConverter(path: List[String]) extends PrimitiveConverter {
    val CharsetDecoder = Charset.forName("UTF-8").newDecoder()

    def addInBuffer(value: Any): Unit =
      if (value != null) {
        buffer += (path -> value)
      }


    override def addBinary(value: Binary): Unit = addInBuffer(value)

    override def addFloat(value: Float): Unit = addInBuffer(value)

    override def addDouble(value: Double): Unit = addInBuffer(value)

    override def addInt(value: Int): Unit = addInBuffer(value)

    override def addBoolean(value: Boolean): Unit = addInBuffer(value)

    override def addLong(value: Long): Unit = addInBuffer(value)
  }

//  class ParquetStringConverter(name: String) extends ParquetPrimitiveConverter(name) {
//    override def addBinary(value: Binary): Unit =
//      Option(value).foreach(v => buffer += (name -> v.toStringUsingUTF8))
//  }

}




