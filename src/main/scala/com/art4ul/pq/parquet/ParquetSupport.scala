package com.art4ul.pq.parquet

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport, WriteSupport}
import org.apache.parquet.io.api.{Binary, GroupConverter, RecordConsumer, RecordMaterializer}
import org.apache.parquet.io.{InvalidRecordException, ParquetEncodingException}
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._


object ParquetSupport {

  type ParquetRecord = Map[List[String], Any]

  class ParquetRecordMaterializer(schema: MessageType) extends RecordMaterializer[ParquetRecord] {
    val converter: ParquetRecordConverter = new ParquetRecordConverter(schema)

    override def getRootConverter: GroupConverter = converter

    override def getCurrentRecord: ParquetRecord ={
      converter.getCurrentRecord
    }
  }


  class ParquetReadSupport extends ReadSupport[ParquetRecord] {

    override def prepareForRead(configuration: Configuration,
                                keyValueMetaData: util.Map[String, String],
                                fileSchema: MessageType,
                                readContext: ReadContext): RecordMaterializer[ParquetRecord] =
      return new ParquetRecordMaterializer(fileSchema)

    override def init(context: InitContext): ReadContext = new ReadSupport.ReadContext(context.getFileSchema)
  }

  /**
    *
    * ParquetWriteSupport
    */
//  class ParquetWriteSupport(schema: MessageType) extends WriteSupport[ParquetRecord] {
//
//    private var recordConsumer: RecordConsumer = _
//
//    override def init(configuration: Configuration): WriteContext =
//      new WriteContext(schema, new java.util.HashMap())
//
//    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
//      this.recordConsumer = recordConsumer
//    }
//
//    override def write(record: ParquetRecord): Unit = {
//      recordConsumer.startMessage
//      record.foreach { case (path: List[String], value) =>
//        try {
//          val columnDesc = schema.getColumnDescription(path)
//          recordConsumer.startField(name, index)
//          val t = schema.getType(index)
//          t.asPrimitiveType().getPrimitiveTypeName match {
//            case INT64 => recordConsumer.addLong(value.asInstanceOf[Long])
//            case INT32 => recordConsumer.addInteger(value.asInstanceOf[Int])
//            case BOOLEAN => recordConsumer.addBoolean(value.asInstanceOf[Boolean])
//            case FLOAT => recordConsumer.addFloat(value.asInstanceOf[Float])
//            case DOUBLE => recordConsumer.addDouble(value.asInstanceOf[Double])
//            case BINARY => recordConsumer.addBinary(value.asInstanceOf[Binary])
//            case t => throw new ParquetEncodingException(s"Unsuported type:$t")
//          }
//
//          recordConsumer.endField(name, schema.getFieldIndex(name))
//        } catch {
//          case ex: InvalidRecordException =>
//            println(s"Field $name is not found in schema and will be ignored")
//        }
//      }
//    }
//
//  }

}