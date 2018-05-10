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
