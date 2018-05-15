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

import com.art4ul.pq.sql._
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}

class ParquetFilterConverter(schema: MessageType) {

  val INT32 = "INT32"
  val INT64 = "INT64"
  val FLOAT = "FLOAT"
  val DOUBLE = "DOUBLE"
  val BOOLEAN = "BOOLEAN"
  val FIXED_LEN_BYTE_ARRAY = "FIXED_LEN_BYTE_ARRAY"

  def getType(column: String): PrimitiveType = {
    val res: Type = schema.getType(schema.getFieldIndex(column))
    res.asPrimitiveType()
  }

  private def cleanString(str: String): String = {
    val FolderPattern = """^\'(.*)\'$""".r
    FolderPattern
      .findFirstMatchIn(str)
      .map(_.group(1))
      .getOrElse(str)
  }

  def intValue(value: String): java.lang.Integer = {
    Int.box(value.toInt.intValue())
  }

  def longValue(value: String): java.lang.Long = {
    Long.box(value.toLong.intValue())
  }

  def floatValue(value: String): java.lang.Float = {
    Float.box(value.toFloat.intValue())
  }

  def doubleValue(value: String): java.lang.Double = {
    Double.box(value.toDouble.intValue())
  }

  def binaryValue(value: String): Binary = {
    Binary.fromString(cleanString(value))
  }

  def booleanValue(value: String): java.lang.Boolean = {
    Boolean.box(value.toBoolean)
  }

  def eqCol(column: Ast): FilterPredicate = column match {
    case ColumnValue(name, value) =>
      getType(name).getPrimitiveTypeName.name() match {
        case INT32 => FilterApi.eq(FilterApi.intColumn(name), intValue(value))
        case INT64 => FilterApi.eq(FilterApi.longColumn(name), longValue(value))
        case FLOAT => FilterApi.eq(FilterApi.floatColumn(name), floatValue(value))
        case DOUBLE => FilterApi.eq(FilterApi.doubleColumn(name), doubleValue(value))
        case BOOLEAN => FilterApi.eq(FilterApi.booleanColumn(name), booleanValue(value))
        case FIXED_LEN_BYTE_ARRAY => FilterApi.eq(FilterApi.binaryColumn(name), binaryValue(value))
        case _ => throw new UnsupportedOperationException()
      }

    case _ => throw new UnsupportedOperationException()
  }

  def notEq(column: Ast): FilterPredicate = column match {
    case ColumnValue(name, value) =>
      getType(name).getPrimitiveTypeName.name() match {
        case INT32 => FilterApi.notEq(FilterApi.intColumn(name), intValue(value))
        case INT64 => FilterApi.notEq(FilterApi.longColumn(name), longValue(value))
        case FLOAT => FilterApi.notEq(FilterApi.floatColumn(name), floatValue(value))
        case DOUBLE => FilterApi.notEq(FilterApi.doubleColumn(name), doubleValue(value))
        case BOOLEAN => FilterApi.notEq(FilterApi.booleanColumn(name), booleanValue(value))
        case FIXED_LEN_BYTE_ARRAY => FilterApi.notEq(FilterApi.binaryColumn(name), binaryValue(value))
        case _ => throw new UnsupportedOperationException()
      }

    case _ => throw new UnsupportedOperationException()
  }

  def lt(column: Ast): FilterPredicate = column match {
    case ColumnValue(name, value) =>
      getType(name).getPrimitiveTypeName.name() match {
        case INT32 => FilterApi.lt(FilterApi.intColumn(name), intValue(value))
        case INT64 => FilterApi.lt(FilterApi.longColumn(name), longValue(value))
        case FLOAT => FilterApi.lt(FilterApi.floatColumn(name), floatValue(value))
        case DOUBLE => FilterApi.lt(FilterApi.doubleColumn(name), doubleValue(value))
        case FIXED_LEN_BYTE_ARRAY => FilterApi.lt(FilterApi.binaryColumn(name), binaryValue(value))
        case _ => throw new UnsupportedOperationException()
      }

    case _ => throw new UnsupportedOperationException()
  }

  def ltEq(column: Ast): FilterPredicate = column match {
    case ColumnValue(name, value) =>
      getType(name).getPrimitiveTypeName.name() match {
        case INT32 => FilterApi.ltEq(FilterApi.intColumn(name), intValue(value))
        case INT64 => FilterApi.ltEq(FilterApi.longColumn(name), longValue(value))
        case FLOAT => FilterApi.ltEq(FilterApi.floatColumn(name), floatValue(value))
        case DOUBLE => FilterApi.ltEq(FilterApi.doubleColumn(name), doubleValue(value))
        case FIXED_LEN_BYTE_ARRAY => FilterApi.ltEq(FilterApi.binaryColumn(name), binaryValue(value))
        case _ => throw new UnsupportedOperationException()
      }

    case _ => throw new UnsupportedOperationException()
  }

  def gtEq(column: Ast): FilterPredicate = column match {
    case ColumnValue(name, value) =>
      getType(name).getPrimitiveTypeName.name() match {
        case INT32 => FilterApi.gtEq(FilterApi.intColumn(name), intValue(value))
        case INT64 => FilterApi.gtEq(FilterApi.longColumn(name), longValue(value))
        case FLOAT => FilterApi.gtEq(FilterApi.floatColumn(name), floatValue(value))
        case DOUBLE => FilterApi.gtEq(FilterApi.doubleColumn(name), doubleValue(value))
        case FIXED_LEN_BYTE_ARRAY => FilterApi.gtEq(FilterApi.binaryColumn(name), binaryValue(value))
        case _ => throw new UnsupportedOperationException()
      }

    case _ => throw new UnsupportedOperationException()
  }

  def gt(column: Ast): FilterPredicate = column match {
    case ColumnValue(name, value) =>
      getType(name).getPrimitiveTypeName.name() match {
        case INT32 => FilterApi.gt(FilterApi.intColumn(name), intValue(value))
        case INT64 => FilterApi.gt(FilterApi.longColumn(name), longValue(value))
        case FLOAT => FilterApi.gt(FilterApi.floatColumn(name), floatValue(value))
        case DOUBLE => FilterApi.gt(FilterApi.doubleColumn(name), doubleValue(value))
        case FIXED_LEN_BYTE_ARRAY => FilterApi.gt(FilterApi.binaryColumn(name), binaryValue(value))
        case _ => throw new UnsupportedOperationException()
      }

    case _ => throw new UnsupportedOperationException()
  }

  def convert(ast: Ast): FilterPredicate = ast match {
    case And(left, right) => FilterApi.and(convert(left), convert(right))
    case Or(left, right) => FilterApi.or(convert(left), convert(right))
    case Not(first) => FilterApi.not(convert(first))
    case Eq(col) => eqCol(col)
    case Gt(col) => gt(col)
    case GtEq(col) => gtEq(col)
    case NotEq(col) => notEq(col)
    case Lt(col) => lt(col)
    case LtEq(col) => ltEq(col)
  }

}
