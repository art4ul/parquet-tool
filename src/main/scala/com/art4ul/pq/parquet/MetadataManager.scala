package com.art4ul.pq.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

object MetadataManager {

  def readSchemas(paths: Seq[Path])(implicit conf: Configuration): List[MessageType] = {
    val fileStatuses = paths.map(getFileStatus).toList
    ParquetFileReader.readAllFootersInParallel(conf, fileStatuses, false)
      .map(_.getParquetMetadata.getFileMetaData.getSchema).toList
  }

  def getFileStatus(path: Path)(implicit conf: Configuration): FileStatus = {
    val fs = path.getFileSystem(conf)
    fs.getFileStatus(path)
  }

  def mergeSchemas(schemas:List[MessageType]):MessageType ={
    schemas match {
      case head :: Nil =>head
      case head :: tail => tail.fold(head)((acc,el) => acc.union(el))
      case Nil => throw new RuntimeException("")
    }
  }

  def commonSchema(paths: Seq[Path])(implicit conf: Configuration):MessageType ={
    val schemas = readSchemas(paths)
    mergeSchemas(schemas)
  }

}
